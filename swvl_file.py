
class SWVLFile:
    def __init__(self, file_obj):
        self.file_obj = file_obj
        self.current_row = None
        self.current_id = None

    def get_latest_row_by_id(self, id):
        last_pos = self.file_obj.tell()
        next_row = self.file_obj.readline()

        while self.get_id_from_row(next_row) == id:
            last_pos = self.file_obj.tell()
            self.current_row = next_row
            self.current_id = self.get_id_from_row(next_row)
            next_row = self.file_obj.readline()

        self.file_obj.seek(last_pos)

        if self.current_id == id:
            return self.current_row
        else:
            return None

    def get_next_row(self):
        last_pos = self.file_obj.tell()
        next_row = self.file_obj.readline()
        self.file_obj.seek(last_pos)
        return next_row

    def get_id_from_row(self, row):
        return int(row.split(",")[0]) if row else None
        
