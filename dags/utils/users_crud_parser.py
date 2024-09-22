import pandas as pd
import os
import datetime

from airflow.utils.log.logging_mixin import LoggingMixin


logger = LoggingMixin().log


def process_daily_log(filename: str) -> pd.DataFrame:
    """
        Обрабатывает файл с логами за один день и возвращает DataFrame
        с количеством действий каждого пользователя.
    Args:
        filename (str): Путь к файлу с логированием.

    Returns:
        pd.DataFrame: DataFrame с количеством действий каждого пользователя.
    """

    try:
        df = pd.read_csv(filename, header=None, names=['email', 'action', 'dt'])
        daily_counts = df.groupby('email')['action'].value_counts().unstack(fill_value=0)
        daily_counts.columns = ['create_count', 'read_count', 'update_count', 'delete_count']
        return daily_counts

    except Exception as e:
        logger.error(f"Ошибка при обработке файла {filename}: {e}")
        return pd.DataFrame(columns=['create_count', 'read_count', 'update_count', 'delete_count'])


def calculate_daily_aggregates(current_data: datetime.date, log_dir: str, date_count: int) -> bool:
    """
        Args:
            current_data (datetime.date): Дата, от которой будут вычислены действия пользователей за 7 предыдущих дней
            log_dir (str): Путь к директории с файлами логов.
            date_count (int): Количество дней выборки
        Returns:
            bool: Возвращает True, если агрегация информации прошла успешно, и False, если возникла ошибка.
    """
    try:
        logger.info(f'->Запускаем агрегацию информации по дням от {current_data} <-')

        filenames = [os.path.join(log_dir, f'{current_data - datetime.timedelta(days=i)}.csv')
                     for i in range(date_count)]
        if not all(os.path.exists(filename) for filename in filenames):
            raise Exception('Отсутствуют необходимые файлы логов для агрегации')

        for filename in filenames:
            if os.path.exists(filename):
                daily_counts = process_daily_log(filename)
                daily_counts.to_csv(os.path.join(log_dir, f'intermediate/intermediate_{os.path.basename(filename)}'),
                                    index=True)
        logger.info(f'->Агрегация информации по дням завершена успешно <-')
        return True

    except Exception as e:
        logger.error(f'Ошибка при агрегации информации по дням: {e}')
        raise e


def calculate_weekly_aggregates(current_data: datetime.date, log_dir: str, output_dir: str, date_count: int) -> bool:
    """
    Args:
        current_data (datetime.date): Дата, от которой будут вычислены действия пользователей за 7 предыдущих дней
        log_dir (str): Путь к директории с файлами логов
        output_dir (str): Путь к директории с результатом
        date_count (int): Количество дней выборки
    Returns:
        bool: Возвращает True, если агрегация информации прошла успешно, и False, если возникла ошибка.
    """
    try:
        logger.info(f'->Запускаем агрегацию информации для пользователей за 7 предыдущих дней от {current_data} <-')

        intermediate_filenames = [
            os.path.join(log_dir, 'intermediate', f'intermediate_{current_data - datetime.timedelta(days=i)}.csv')
            for i in range(date_count)
        ]

        if not all(os.path.exists(filename) for filename in intermediate_filenames):
            raise Exception('Ошибка при агрегации информации (файлы intermediate) для нужных дат не найдены.')

        aggregated_data = pd.DataFrame()
        for filename in intermediate_filenames:
            daily_data = pd.read_csv(filename, index_col=0)
            aggregated_data = aggregated_data.add(daily_data, fill_value=0)

        aggregated_data.to_csv(os.path.join(output_dir, f"{current_data.strftime('%Y-%m-%d')}.csv"), index=True)
        logger.info(f'->Агрегация информации завершена успешно <-')
        return True

    except Exception as e:
        logger.error(f'Ошибка при агрегации информации: {e}')
        raise e


if __name__ == '__main__':
    calculate_daily_aggregates(datetime.date.today(), './../input', 7)
    calculate_weekly_aggregates(datetime.date.today(), './../input',  './../output', 7)
