from sys import platform
import argparse
from datetime import datetime, timedelta
import os
import ftplib
import configparser
import mysqldb
import request_handler
import requests
import gnsscal
import re
import math


def os_dependency_slash() -> str:
    if platform == 'win32':
        return '\\'
    return '/'


class SessionWithHeaderRedirection(requests.Session):
    AUTH_HOST = 'urs.earthdata.nasa.gov'

    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)

    # Overrides from the library to keep headers when redirected to or from the NASA auth host.

    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        url = prepared_request.url
        if 'Authorization' in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)
            if (original_parsed.hostname != redirect_parsed.hostname) and \
                    redirect_parsed.hostname != self.AUTH_HOST and \
                    original_parsed.hostname != self.AUTH_HOST:
                del headers['Authorization']

        return


def download(url: str, file_path='', attempts=1) -> str:
    """Downloads a URL content into a file (with large file support by streaming)

    :param url: URL to download
    :param file_path: Local file name to contain the data downloaded
    :param attempts: Number of attempts
    :return: New file path. Empty string if the download failed
    """
    if not file_path:
        file_path = os.path.realpath(os.path.basename(url))
    print(f'Downloading {url} content to {file_path}')
    for attempt in range(1, attempts + 1):
        try:
            # if attempt > 1:
            # time.sleep(1)  # 10 seconds wait time between downloads
            with session.get(url, stream=True) as response:
                response.raise_for_status()
                with open(file_path, 'wb') as out_file:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                        out_file.write(chunk)
                print('Download finished successfully')
                return file_path
        except Exception as ex:
            print(f'Attempt #{attempt} failed with error: {ex}')
    return file_path


def length_calculator(num: int) -> int:
    if num < 10:
        return 1
    return 1 + length_calculator(int(num / 10))


def get_doy_str(num: int) -> str:
    day = "000"
    if length_calculator(num) == 3:
        day = str(num)
    elif length_calculator(num) == 2:
        day = "0" + str(num)
    elif length_calculator(num) == 1:
        day = "00" + str(num)
    return day


def get_calculation_epoch() -> datetime:
    selected_scenario_data = handler.select_scenario_data(scenario_id)
    if len(selected_scenario_data) == 0:
        print(f"In the database {db_name} no data for scenario_id {scenario_id}")
        database.close_connection()
        exit()
    return selected_scenario_data[0][4]


def get_list_stations() -> list:
    result = list()
    with ftplib.FTP(ftp_server) as ftp:
        ftp.login(ftp_login, ftp_pass)
        d_o_y = get_doy_str(epoch.timetuple().tm_yday)
        ftp.cwd(f"LOCAL/FREE/CDIS/DATA/DAILY/{epoch.year}/{d_o_y}/{epoch.strftime('%Y')[2:]}o")
        for file in ftp.nlst():
            result.append(file[:4])
    return result


def gunzip(file):
    exit_status = os.system('gunzip -f ' + file)

    if exit_status != 0:
        print(f"ERROR: Could not gunzip file {file}!")

    return exit_status


def parse(file: str) -> dict:
    result = dict()
    in_solution_estimate_section = False
    try:
        with open(file, 'r') as f:
            start_solution_estimate_pattern = re.compile('^\+SOLUTION/ESTIMATE.*')
            end_solution_estimate_pattern = re.compile('^\-SOLUTION\/ESTIMATE.*')
            station_coordinate_pattern = re.compile(
                '^\s+\d+\s+STA(\w)\s+(\w+)\s+(\w).*\d+\s+(-?[\d+]?\.\d+[Ee][+-]?\d+)\s+(-?[\d+]?\.\d+[Ee][+-]?\d+)$')
            for line in f:
                start_solution_estimate_match = start_solution_estimate_pattern.findall(line)
                end_solution_estimate_match = end_solution_estimate_pattern.findall(line)

                if start_solution_estimate_match:
                    in_solution_estimate_section = True
                    continue
                elif end_solution_estimate_match:
                    in_solution_estimate_section = False
                    break

                if in_solution_estimate_section:
                    station_coordinate_match = station_coordinate_pattern.findall(line)
                    if station_coordinate_match and stations.count(station_coordinate_match[0][1].lower()):
                        if not station_coordinate_match[0][1].lower() in result.keys():
                            coord_data = request_handler.Coordinates()
                            coord_data.name = station_coordinate_match[0][1].lower()
                            coord_data.dt = epoch
                            result[station_coordinate_match[0][1].lower()] = coord_data
                        if station_coordinate_match[0][0] == 'X':
                            result[station_coordinate_match[0][1].lower()].x = float(station_coordinate_match[0][3])
                        elif station_coordinate_match[0][0] == 'Y':
                            result[station_coordinate_match[0][1].lower()].y = float(station_coordinate_match[0][3])
                        else:
                            result[station_coordinate_match[0][1].lower()].z = float(station_coordinate_match[0][3])
    except Exception as ex:
        print(f"Failed with error: {ex}")
        raise
    return result


def ecef2blh(x: float, y: float, z: float) -> list:
    PI_180 = math.pi / 180.0
    # WGS84 座標パラメータ
    A = 6378137.0
    ONE_F = 298.257223563
    B = A * (1.0 - 1.0 / ONE_F)
    E2 = (1.0 / ONE_F) * (2 - (1.0 / ONE_F))
    # e^2 = 2 * f - f * f
    #     = (a^2 - b^2) / a^2
    ED2 = E2 * A * A / (B * B)  # e'^2= (a^2 - b^2) / b^2
    try:
        n = lambda x: A / \
                      math.sqrt(1.0 - E2 * math.sin(x * PI_180) ** 2)
        p = math.sqrt(x * x + y * y)
        theta = math.atan2(z * A, p * B) / PI_180
        lat = math.atan2(
            z + ED2 * B * math.sin(theta * PI_180) ** 3,
            p - E2 * A * math.cos(theta * PI_180) ** 3
        ) / PI_180
        lon = math.atan2(y, x) / PI_180
        ht = (p / math.cos(lat * PI_180)) - n(lat)
        return [lat, lon, ht]
    except Exception as e:
        raise


def fill_geocentric_coordinates(data: dict) -> None:
    for value in data.values():
        blh = ecef2blh(value.x, value.y, value.z)
        value.latitude = blh[0]
        value.longitude = blh[1]
        value.height = blh[2]


def check_station_id() -> bool:
    if len(scenario_id) == 0:
        print(f"Verification error scenario_id. "
              f"You must specify the scenario_id in config.ini")
        return False
    selected_scenario_tb = handler.select_scenario(scenario_id)
    if len(selected_scenario_tb) == 0:
        print(f"Verification error scenario_id. "
              f"Check the correctness of the specified.")
        return False
    return True


def updating_list_stations(set_stations: set) -> None:
    if not check_station_id():
        return
    handler.delete_stations(scenario_id)
    for station in set_stations:
        handler.insert_station(scenario_id, station)


def sending_data_db(data: dict) -> None:
    set_stations = set()
    for key in data.keys():
        set_stations.add(key)
        selected_station_data = handler.select_station_data(key)
        if len(selected_station_data) == 0:
            # insert
            handler.insert_station_data(data[key])
        else:
            # update
            handler.update_station_data(key, data[key])
    updating_list_stations(set_stations)


def upd_coordinates() -> None:
    gps_week = gnsscal.date2gpswd(epoch.date())
    url_file = f"https://cddis.nasa.gov/archive/gnss/products/{gps_week[0]}" \
               f"/igs{epoch.strftime('%Y')[2:]}P{gps_week[0]}{gps_week[1]}.snx.Z"
    current_file = download(url_file)
    gunzip(current_file)

    result_parse = parse(os.path.basename(current_file)[:-2])
    fill_geocentric_coordinates(result_parse)

    # отправка данных в БД odtssw_paf
    sending_data_db(result_parse)

    # удаления tmp_ каталога
    os.remove(current_file[:-2])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Processing of the result of station coordinates refinement (IAC)')
    parser.add_argument('--scenario_id', type=str, dest='scenario_id',
                        default='011888',
                        help="scenario_id for which you want to update the set of stations")

    args = parser.parse_args()
    scenario_id = args.scenario_id

    # Чтение config-файла
    config = configparser.ConfigParser(allow_no_value=True)
    config.read('config_iac.ini')
    host = config['Database']['address']
    db_name = config['Database']['db_name']
    username = config['Database']['username']
    password = config['Database']['password']
    port = config.getint('Database', 'port')
    ssh_host = config['SSH']['ssh_host']
    ssh_port = config.getint('SSH', 'ssh_port')
    ssh_user = config['SSH']['ssh_user']
    ssh_password = config['SSH']['ssh_password']
    ftp_server = config['FTP']['address']
    ftp_login = config['FTP']['username']
    ftp_pass = config['FTP']['password']
    cddis_username = config['CDDIS']['username']
    cddis_password = config['CDDIS']['password']

    database = mysqldb.MySQLConnection(host_name=host, database_name=db_name, user_name=username,
                                       user_password=password, port=port)

    db_connection = database.create_connection_tunnel(ssh_host=ssh_host, ssh_port=ssh_port, ssh_username=ssh_user,
                                                      ssh_password=ssh_password)

    handler = request_handler.RequestHandler(db_connection)
    session = SessionWithHeaderRedirection(cddis_username, cddis_password)
    epoch = get_calculation_epoch() - timedelta(days=1)

    stations = get_list_stations()
    upd_coordinates()

    # Закрыть подключение к БД
    database.close_connection()
