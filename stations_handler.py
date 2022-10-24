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
import gzip
import shutil
import zlib


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


def uncompress(file):
    exit_status = os.system("compress -fd " + file)

    if exit_status != 0:
        print("ERROR: Could not uncompress file " + file + " !!!")

    return exit_status


def gunzip(file):
    exit_status = os.system('gunzip -f ' + file)

    if exit_status != 0:
        print("ERROR: Could not gunzip file " + file + " !!!")

    return exit_status


def upd_coordinates() -> None:
    gps_week = gnsscal.date2gpswd(epoch.date())
    url_file = f"https://cddis.nasa.gov/archive/gnss/products/{gps_week[0]}" \
               f"/igs{epoch.strftime('%Y')[2:]}P{gps_week[0]}{gps_week[1]}.snx.Z"
    current_file = download(url_file)

    if current_file[-2:] == "gz":
        gunzip(current_file)
        # self.snxFilePath = self.snxFilePath[0:-3]
        # wasZipped = True

        # check for unix compression
    elif current_file[-1:] == "Z":
        uncompress(current_file)
        # self.snxFilePath = self.snxFilePath[0:-2]
        # wasCompressed = True

    # удаления tmp_ каталога
    shutil.rmtree(os.path.dirname(current_file) + os_dependency_slash() + 'tmp_', ignore_errors=True)


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

    # Закрыть подключение к БД
    # database.close_connection()

    # if len(date.split('.')) == 3:
    #     try:
    #         datetime.datetime.strptime(date, '%Y.%m.%d')
    #
    #     except Exception as e:
    #         print('Invalid date format.', e)

    stations = get_list_stations()
    upd_coordinates()

    # отправка данных в БД odtssw_paf
    # sending_data_database(parsed_data)

    # Закрыть подключение к БД
    database.close_connection()
