import os
import datetime
import shutil

from pyspark.sql import SparkSession
from airflow.utils.log.logging_mixin import LoggingMixin


logger = LoggingMixin().log


def move_and_rename_csv(output_path: str) -> bool:
    """
        Перемещает и переименовывает сгенерированный CSV-файл из указанной директории.

        Args::
            - output_path (str): Путь к директории, содержащей сгенерированные CSV-файлы.
        """
    try:
        files = os.listdir(output_path)

        for file in files:
            if file.endswith('.csv'):
                original_file_path = os.path.join(output_path, file)
                new_file_name = f"{output_path}.csv"
                shutil.move(original_file_path, new_file_name)
                return True

    except Exception as e:
        logger.error(f"Ошибка во время переименования файла: {e}")
        raise e


def process_daily_log(spark: SparkSession, filename: str) -> bool:
    """
        Обрабатывает файл с логами за один день и возвращает DataFrame
        с количеством действий каждого пользователя.
    Args:
        spark (SparkSession): Spark сессия
        filename (str): Путь к файлу с логированием.

    Returns:
        bool: True, если обработка прошла успешно, иначе False.
    """

    try:
        df = spark.read.csv(filename, header=False, inferSchema=True)
        df = df.toDF("email", "action", "dt")

        daily_counts = df.groupBy("email", "action").count().withColumnRenamed("count", "action_count")
        daily_counts = daily_counts.groupBy("email").pivot("action").sum("action_count").fillna(0)

        path_list = filename.replace('.csv', '_daily_counts.parquet').split(os.sep)
        output_file_path = os.path.join(os.sep, *path_list[:-1], "intermediate", path_list[-1])
        daily_counts.write.mode("overwrite").parquet(output_file_path)
        return True

    except Exception as e:
        logger.error(f"Ошибка при обработке файла {filename}: {e}")
        return False


def calculate_daily_aggregates(spark: SparkSession, current_data: datetime.date, log_dir: str, date_count: int) -> bool:
    """
        Args:
            spark (SparkSession): Spark сессия
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
            spark.stop()
            raise Exception('Отсутствуют необходимые файлы логов для агрегации')

        for filename in filenames:
            if not process_daily_log(spark, filename):
                spark.stop()
                return False

        print(f'->Агрегация информации по дням завершена успешно <-')
        spark.stop()
        return True

    except Exception as e:
        logger.error(f'Ошибка при агрегации информации по дням: {e}')
        spark.stop()
        raise e


def calculate_weekly_aggregates(spark: SparkSession, current_data: datetime.date, log_dir: str, output_dir: str,
                                date_count: int) -> bool:
    """
    Вычисляет суммарные количества действий пользователей за указанное количество предыдущих дней
    и записывает их в выходной файл.

    Args:
        spark (SparkSession): Spark сессия
        current_data (datetime.date): Дата, от которой будут вычислены действия пользователей за 7 предыдущих дней
        log_dir (str): Путь к директории с файлами логов
        output_dir (str): Путь к директории с результатом
        date_count (int): Количество дней выборки

    Returns:
        bool: True, если агрегация информации прошла успешно, и False, если возникла ошибка.
    """
    try:
        logger.info(f'->Запускаем агрегацию информации для пользователей за {date_count} '
                    f'предыдущих дней от {current_data} <-')

        intermediate_filenames = [
            os.path.join(log_dir, 'intermediate',
                         f'{current_data - datetime.timedelta(days=i)}_daily_counts.parquet')
            for i in range(date_count)
        ]

        aggregated_data = spark.read.parquet(*intermediate_filenames)
        aggregated_data = aggregated_data.groupBy("email").sum(*[c for c in aggregated_data.columns if c != "email"])
        aggregated_data = aggregated_data.withColumnRenamed('sum(CREATE)', 'create_count') \
            .withColumnRenamed('sum(READ)', 'read_count') \
            .withColumnRenamed('sum(UPDATE)', 'update_count') \
            .withColumnRenamed('sum(DELETE)', 'delete_count')

        output_filename = os.path.join(output_dir, f"{current_data.strftime('%Y-%m-%d')}")
        aggregated_data.write.mode("overwrite").option("delimiter", ";").csv(output_filename, header=True)
        move_and_rename_csv(output_filename)

        logger.info(f'->Агрегация информации завершена успешно <-')
        spark.stop()
        return True

    except Exception as e:
        logger.error(f'Ошибка при агрегации информации: {e}')
        spark.stop()
        raise e
