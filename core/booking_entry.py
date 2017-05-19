
class BookingEntry():
    def __init__(self):
        self.booking_number = None
        self.first_name = None
        self.middle_name = None
        self.last_name = None
        self.race = None
        self.sex = None
        self.dob = None
        self.booking_date = None
        self.release_date = None
        self.location = None

    def __str__(self):
        return ','.join([value for property, value in vars(self).items()])
