from datetime import datetime
import os

class time_upadate(object):
    def __init__(self,date_upadate,path_to_file_upload_db,path_to_probability_updata_db):
        self.date_upadate=date_upadate
        self.path_to_file_upload_db=path_to_file_upload_db
        self.path_to_probability_updata_db=path_to_probability_updata_db

    def timeng(self):
        while(True):
            time_now=datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S")
            if time_now==datetime.strftime(datetime(int(str(time_now)[0:4]), int(str(time_now)[5:7]),
                                                    self.date_upadate, 0, 0),"%Y.%m.%d %H:%M:%S"):
                print(datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S"))
                os.system(self.path_to_file_upload_db)
                os.system(self.path_to_probability_updata_db)
                break

if __name__ == "__main__":
    time_upadate(date_upadate=30,
                 path_to_file_upload_db='<path to uploading data base code>',
                 path_to_probability_updata_db='main.py').timeng()