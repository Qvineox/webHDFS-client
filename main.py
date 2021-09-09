import json
import os
import sys

import requests

console_config = dict()

# текущая директория HDFS
current_directory = ''


def sync_config():
    print('> Syncing configuration file...')
    with open('config.json') as json_file:
        data = json.load(json_file)

        global console_config
        console_config = data

    print('> Syncing complete.')


def print_error(string):
    print('\033[91m' + string + '\033[0m')


def print_success(string):
    print('\033[92m' + string + '\033[0m')


def send_command(user_input):
    global console_config, current_directory

    command_split = user_input.split(' ')

    command = command_split[0]

    # формируем переменные для запроса
    payload = {
        'user.name': console_config['user']
    }

    # собираем URL-адрес из конфигурационного файла
    url = f"http://{console_config['host']}:{console_config['port']}/webhdfs/v1/{current_directory}"
    print(url)

    # загрузка файла в HDFS
    if command == 'upload':
        payload['op'] = 'CREATE'

        if command_split[1]:
            if command_split[1][0] == '/':
                url = f"http://{console_config['host']}:{console_config['port']}/webhdfs/v1{command_split[1]}"
            if url[-1] == '/':
                url += f"{command_split[1]}"
            else:
                url += f"/{command_split[1]}"

            request = requests.put(url, params=payload)
            # print(request.status_code)

            if request.status_code == 201:
                print_success('> HDFS file created.')

                files = {'upload_file': open(command_split[1], 'rb')}

                payload['op'] = 'APPEND'
                try:
                    request = requests.post(url, files=files, params=payload, timeout=5)
                except requests.exceptions.ConnectionError as e:
                    print(e)
                    return False

                if request.status_code == 200:
                    print_success('> File uploaded successfully.')
                    return True
                else:
                    print_error('> Failed to upload file.')
                    return False
            else:
                print_error(f"> Error code: {request.status_code}")
                return False

    # создание директории HDFS
    if command == 'mkdir':
        payload['op'] = 'MKDIRS'

        if len(command_split) > 1:
            if command_split[1][0] == '/':
                url = f"http://{console_config['host']}:{console_config['port']}/webhdfs/v1{command_split[1]}"
            else:
                if url[-1] == '/':
                    url += f"{command_split[1]}"
                else:
                    url += f"/{command_split[1]}"

        request = requests.put(url, params=payload)

        print(request.url)

        data = json.loads(request.text)
        try:
            if data['boolean']:
                print_success(f'> Directory /{command_split[1]} created.')
                return True
        except:
            print_error(f"> Error: {data['RemoteException']['exception']}")
            return False

    # удаление файлов
    elif command == 'rm':
        payload['op'] = 'DELETE'

        # рекусривное удаление
        if len(command_split) > 2 and command_split[2] == '-r':
            payload['recursive'] = 'TRUE'

        if command_split[1][0] == '/':
            url = f"http://{console_config['host']}:{console_config['port']}/webhdfs/v1{command_split[1]}"

            request = requests.delete(url, params=payload)

            data = json.loads(request.text)

            try:
                if data['boolean']:
                    print_success(f'> Deleted /{command_split[1]}.')
                    return True
            except:
                print_error(f"> Error: {data['RemoteException']['exception']}")
                return False
        else:
            url += command_split[1]
            request = requests.delete(url, params=payload)

            data = json.loads(request.text)

            try:
                if data['boolean']:
                    print_success(f'> Deleted /{command_split[1]}.')
                    return True
            except:
                print_error(f"> Error: {data['RemoteException']['exception']}")
                return False

    # перечисление файлов и директорий в директории HDFS
    elif command == 'ls':
        payload['op'] = 'LISTSTATUS'

        if len(command_split) > 1:
            if command_split[1][0] == '/':
                url = f"http://{console_config['host']}:{console_config['port']}/webhdfs/v1{command_split[1]}"
            else:
                url += f"/{command_split[1]}"

        request = requests.get(url, params=payload)
        print(request.url)
        data = json.loads(request.text)

        if request.status_code == 200:
            clean_data = []

            _ = data['FileStatuses']['FileStatus']

            if len(_) > 0:
                for file in _:
                    if file['type'] == 'DIRECTORY':
                        clean_data.append(f"/{file['pathSuffix']}")
                    else:
                        clean_data.append(f"\033[96m{file['pathSuffix']}\033[0m")

                print(' '.join([str(elem) for elem in clean_data]))
            else:
                print('\033[93mEmpty directory.\033[0m')

            return True
        else:
            print_error(f"> Error: {data['RemoteException']['exception']}")
            return False

    # перечисление локальных файлов и директорий
    elif command == 'lls':
        _ = os.listdir(os.getcwd())

        data = []

        for item in _:
            if os.path.isfile(item):
                data.append('\033[96m' + item + '\033[0m')
            elif os.path.isdir(item):
                data.append(f"/{item}")

        print(' '.join([str(elem) for elem in data]))
        return True

    # переход в директорию HDFS
    elif command == 'cd':
        payload['op'] = 'LISTSTATUS'

        if len(command_split) > 1:
            if command_split[1] == '..':
                _ = '/'.join(current_directory.split('/')[:-1])
                current_directory = _.replace("//", "/")
                return True
            elif command_split[1][0] == '/':
                current_directory = command_split[1][1:]
                return True
            else:
                current_directory += f"{command_split[1]}"
                return True
        else:
            current_directory = ''
            return True

    # переход в локальную директорию
    elif command == 'lcd':
        try:
            os.chdir(command_split[1])
            return True
        except FileNotFoundError:
            print_error(f"> Error: {sys.exc_info()}")
            return False

    # выход и завершение работы программы
    elif command == 'exit':
        return None

    else:
        return False


if __name__ == '__main__':
    print('Welcome to WebHDFS Console!')

    sync_config()

    callback = True

    print('Enter help to see all commands. Enter exit to close.')

    while callback is not None:
        print(f"Current directory: {current_directory}. Local: {os.getcwd()}. Enter command...")

        callback = send_command(input())
        if callback:
            print('\033[92m' + '> Success' + '\033[0m')
        else:
            print('\033[91m' + '> Failure' + '\033[0m')
        # print(callback)
