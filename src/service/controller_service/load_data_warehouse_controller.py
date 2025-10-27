from src.config.procedure import get_log_load_warehouse, insert_log_warehouse
from src.service.controller_service.database_controller import Controller
from src.service.load_data_warehourse_service.load_data_warehouse import LoadDataWarehouse


class LoadDataWarehouseController(Controller):
    def __init__(self):
        super().__init__()

    def get_config(self):
        data = self.call_controller_procedure(get_log_load_warehouse, ())

        if data is None:
            return

        load_warehouse = LoadDataWarehouse(
            source_name=data['source_name'],
            file_format=data['file_format'],
            prefix=data['prefix'],
            error_dir_path=data['error_dir_path'],
            controller=self
        )

        result = load_warehouse.handle()

        self.call_controller_procedure(insert_log_warehouse, (
            data['id'],
            result['count_row'],
            result['error_file_name'],
            result['status']
        ))


if __name__ == '__main__':
    c = LoadDataWarehouseController()
    c.get_config()
