import json

class Variables:
    @staticmethod
    def get_variable(name):
        try:
            with open("C:/Users/nehar/ETLProject/config/config.json", "r") as file:
                file_content = json.loads(file.read())
                return file_content[name]
        except Exception as e:
            print(f"[Error]:{e}")


