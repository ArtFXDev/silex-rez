class UserInfo:

    def __init__(self) -> None:
        self.user = ""
        self.name = ""
        self.organization = ""
        self.machineUser = ""
    
    @classmethod
    def from_user_text(cls, userText: str):
        self = cls()
        lines = userText.split("\n")
        for line in lines:
            
            if not line:
                continue

            key, value = [entry.strip() for entry in line.split(":")]

            if key == "user":
                self.user = value
            if key == "name":
                self.name = value
            if key == "organization":
                self.organization = value
            if key == "machineUser":
                self.machineUser = value
        return self
    
    def __str__(self) -> str:
        return f"[ user: {self.user}, name: {self.name}, organization: {self.organization}, machineUser: {self.machineUser} ]"