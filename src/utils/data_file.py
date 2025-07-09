from os import path
import os

class DataFile:
    def __init__(self, filename: str, content: str):
        self.filename = filename
        self.content = content

    def write(self, dest: str, data_category: str):
        os.makedirs(path.join(dest, data_category), exist_ok=True)
        with open(path.join(dest, data_category, self.filename), 'w', encoding='utf-8') as file:
            file.write(self.content)