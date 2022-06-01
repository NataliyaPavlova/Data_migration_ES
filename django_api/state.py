class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        try:
            with open(file_path, 'w'):
                pass
            self.file_path = file_path
        except IOError:
            self.file_path = ''

    def retrieve_state(self) -> dict:
        try:
            with open(self.file_path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError, IOError) as e:
            return {}

    def save_state(self, state: dict) -> None:
        states = self.retrieve_state()
        states.update(state)
        try:
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(states, f)
        except FileNotFoundError:
            pass


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        if value:
            self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        states = self.storage.retrieve_state()
        return states if states == {} else states.get(key)
