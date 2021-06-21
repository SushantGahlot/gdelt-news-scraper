import os
import csv
from threading import Thread
from multiprocessing import Process

class ArticleWriter(Process):
    def __init__(self, writer_queue, csv_file_location=None):
        super(ArticleWriter, self).__init__()
        self.writer_queue = writer_queue
        self.csv_file_location = self._get_csv_file_location(csv_file_location)

    def _get_csv_file_location(self, csv_file_location):
        default_filename = "articles.csv"
        if csv_file_location is not None:
            if os.path.exists(csv_file_location):
                if os.path.isdir(csv_file_location):
                    return os.path.join(csv_file_location, default_filename)
                elif os.path.isfile(csv_file_location):
                    return csv_file_location
        return os.path.join(os.getcwd(), default_filename)


    def _consumer(self):
        f = open(self.csv_file_location, "a+")
        writer = csv.writer(f)
        rows = []

        while True:
            data = self.writer_queue.get()

            if data is None:
                if len(rows):
                    writer.writerows(rows)
                f.close()
                print("Goodbye from csv writer.")
                break

            if len(rows) >= 500:
                writer.writerows(rows)
                rows = []
                f.flush()
                print("Successfully wrote data to csv file.")
            rows.append(data.values())
    
    def run(self):
        c = Thread(target=self._consumer)
        c.start()
        c.join()
