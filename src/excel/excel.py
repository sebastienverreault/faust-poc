import traceback
from datetime import datetime

from src.WebLogs.ReducedLog import ReducedLogV2


class ExcelDriver:
    filename: str = 'reduced_weblogs_' + str(datetime.now()) + '.csv'

    def __init__(self):
        self.list_of_record = []
        self.is_saved = False

    def __del__(self):
        if len(self.list_of_record) > 0 and not self.is_saved:
            self.save()

    def add_record_to_file(self, rlv2: ReducedLogV2):
        self.list_of_record.append(rlv2)
        self.is_saved = False

    def save(self):
        if len(self.list_of_record) > 0:
            try:
                with open(self.filename, 'w') as f:
                    f.write(f'ip_address\tuser_agent\trequest\tbyte_ranges\n')
                    for rlv2 in self.list_of_record:
                        f.write(f'{rlv2.IpAddress}\t{rlv2.UserAgent}\t{rlv2.Request}')
                        for r in rlv2.ByteRanges:
                            f.write(f'\t{str(r)}')
                        f.write("\n")
                self.is_saved = True
            except Exception as ex:
                track = traceback.format_exc()
                print(track)
