from functools import partial
from datetime import datetime
from swvl_file import SWVLFile

class SWVLFilePool:
    def __init__(self, swvl_files):
        self.swvl_files = swvl_files
        self.first_run = True
        self.current_id = None
        self.next_id = None


    def get_next_row(self):
        if self.current_id == None and self.first_run: #first run
            self.first_run = False
            self.current_id = self.get_next_id(None)
        else:
            next_id = self.get_next_id(self.current_id)

            if next_id:
                self.current_id = next_id
            else:
                return None

        latest_rows = list(map(lambda swvl_file: swvl_file.get_latest_row_by_id(self.current_id) , self.swvl_files))
        latest_rows = [r for r in latest_rows if r is not None]
        if len(latest_rows) == 0:
            return None

        sorted(latest_rows, key=lambda r: datetime.strptime(r.split(",")[-1].strip("\n"), '%Y-%m-%dT%H:%M:%S'), reverse=True)

        return latest_rows[0]

    def get_next_id(self, prev_id):
        next_rows = list(map(lambda swvl_file: swvl_file.get_next_row() , self.swvl_files))
        ids = map(lambda r: r.split(",")[0], filter(bool, next_rows))
        ids = sorted(list(map(int, set(ids)))) #uniqe and sorted
        if prev_id:
            ids = list(filter(lambda i: int(i) != int(prev_id), ids))
        if len(ids) > 0:
            return ids[0]
        return None
        