import os

from src.config.procedure import load_staging_warehouse_batdongsan_com_vn, load_staging_warehouse_muaban_net
from src.service.AppException import AppException, LEVEL
from src.service.controller_service.database_controller import Controller
from src.service.notification_service.email import sent_mail, EmailCategory


class LoadDataWarehouse:
    def __init__(self,
                 source_name,
                 controller: Controller,
                 prefix,
                 file_format,
                 error_dir_path):
        self._source = source_name
        self._controller = controller
        self._prefix = prefix
        self._file_format = file_format
        self._error_dir_path = error_dir_path

    def handle(self):
        count_row = 0
        try:
            if self._source == 'batdongsan.com.vn':
                result = self._controller.call_warehouse_procedure(
                    load_staging_warehouse_batdongsan_com_vn,
                    (),
                    None
                )
                count_row = result.get('count_row', 0) if result else 0

            elif self._source == 'muaban.net/bat-dong-san':
                result = self._controller.call_warehouse_procedure(
                    load_staging_warehouse_muaban_net,
                    (),
                    None
                )
                count_row = result.get('count_row', 0) if result else 0
            else:
                raise AppException(
                    level=LEVEL.WAREHOUSE_ERROR,
                    message='Source not found'
                )

            return self.handle_success(count_row)
        except AppException as e:
            return self.handle_exception(e)
        except Exception as e:
            app_exception = AppException(
                level=LEVEL.WAREHOUSE_ERROR,
                message=str(e)
            )
            return self.handle_exception(app_exception)

    def handle_success(self, count_row):
        sent_mail(f"""
                    LOAD DATA WAREHOUSE SUCCESS
                    Source: {self._source}
                    Rows loaded: {count_row}
                    Status: WAREHOUSE_SUCCESS -> DATAMART_PENDING
                    """, EmailCategory.INFO)

        return {
            'file': None,
            'error_file_name': None,
            'count_row': count_row,
            'status': 'WAREHOUSE_SUCCESS -> DATAMART_PENDING'
        }

    def handle_exception(self, exception: AppException):
        filename = f"{self._prefix}{self._file_format}.log"
        path = os.path.join(self._error_dir_path, filename)

        exception.file_error = path
        exception.handle_exception()

        return {
            'file': None,
            'error_file_name': path,
            'count_row': 0,
            'status': 'WAREHOUSE_ERROR'
        }
