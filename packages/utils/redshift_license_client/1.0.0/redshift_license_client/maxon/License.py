from datetime import datetime
from typing import List
import re

MAXON_LICENSE = {
    "CINEVERSTY": "net.maxon.license.service.cineversity",
    "STUDENT": "net.maxon.license.app.bundle_maxonone-release~student",
    "FORGER": "net.maxon.license.app.forger~commercial"
}

class DatePeriod:

    def __init__(self, startDate = None, endDate = None) -> None:
        self.start = startDate
        self.end = endDate
    
    @classmethod
    def from_user_text(cls, startText, endText):
        self = cls()
        self.start = datetime.strptime(startText, "%Y-%m-%d")
        self.end = datetime.strptime(endText, "%Y-%m-%d")
        return self
    
    def __contains__(self, date):
        if self.start is None or self.end is None:
            return False
        return date > self.start and date < self.end


class License:

    def __init__(self) -> None:
        self.name = ""
        self.description = ""
        self.active = False
        self.expired = False
        self.validityPeriod = DatePeriod()
    
    @classmethod
    def from_user_text(cls, userText):
        self = cls()
        token = re.split(r"\s{2,}", userText)
        
        # Assign to attributes
        if len(token) == 5:
            self.name = token[0]
            self.description = token[1]
            self.active = True if token[2] == "yes" else False
            self.expired = True if token[3] == "yes" else False
            startDateText, endDateText = token[4].split(" - ")
            self.validityPeriod = DatePeriod().from_user_text(startDateText, endDateText)
        
        return self

    def __str__(self):
        return f"[{self.description}: {self.active}]"


class LicenseList:

    def __init__(self) -> None:
        self.licenses: List[License] = []
    
    def add_license(self, license: License):
        self.licenses.append(license)
    
    def __contains__(self, licenseName):
        for license in self.licenses:
            if license.name == licenseName:
                return True
        return False
    
    def __getitem__(self, licenseName):
        for license in self.licenses:
            if license.name == licenseName:
                return license
        return KeyError("LicenseName not in LicenseList")
    
    def __str__(self) -> str:
        return "\n".join([str(license) for license in self.licenses])