from src.service.extract_service.crawler.source_A_1_crawler import SourceA1Crawler
from src.service.extract_service.crawler.source_B_1_crawler import SourceB1Crawler


class ExtractService:
    def __init__(self, source_name):
        self._source_name = source_name
        self._crawler = None

    def get_crawler(self):
        if self._source_name == 'batdongsan.com.vn':
            self._crawler = SourceA1Crawler()
        elif self._source_name == 'muaban.net/bat-dong-san':
            self._crawler = SourceB1Crawler()
        else:
            raise ValueError(f"Unknown source: {self._source_name}")

        return self._crawler

    def execute(self, limit_page=None, file_format=None, extension=None,
                prefix=None, data_dir_path=None, error_dir_path=None,
                purpose=None, base_url=None, source_page=None,
                paging_pattern=None, scenario=None, navigate_scenario=None):

        crawler = self.get_crawler()

        if limit_page:
            crawler._limit_page = limit_page
        if file_format:
            crawler._file_format = file_format
        if extension:
            crawler._extension = extension
        if prefix:
            crawler._prefix = prefix
        if data_dir_path:
            crawler._data_dir_path = data_dir_path
        if error_dir_path:
            crawler._error_dir_path = error_dir_path
        if purpose:
            crawler._purpose = purpose
        if base_url:
            crawler._base_url = base_url
        if source_page:
            crawler._source_page = source_page
        if paging_pattern:
            crawler._paging_pattern = paging_pattern
        if scenario:
            crawler._scenario = scenario
        if navigate_scenario:
            crawler._navigate_scenario = navigate_scenario

        return crawler.handle()
