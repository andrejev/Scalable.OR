from scalableor.manager import DataTypeManager


# Helper functions
def check_date(day, month, year):

    # Check year
    year = int(year)
    leap_year = year % 4 == 0

    # Check month
    month = int(month)
    if month < 1 or month > 12:
        return False

    # Check day
    day = int(day)
    if day < 1:
        return False

    # Months with 31 days
    if month in [1, 3, 5, 7, 8, 10, 12]:
        if day > 31:
            return False

    # Months with 30 days
    elif month in [4, 6, 9, 11]:
        if day > 30:
            return False

    # February
    else:
        if leap_year:
            if day > 29:
                return False
        else:
            if day > 28:
                return False

    return True


@DataTypeManager.register("ipv4")
def ipv4(field):

    # IP Address consists of four blocks of up to three digits each
    blocks = field.split(".")

    # It must be exactly four blocks
    if len(blocks) != 4:
        return False

    # Check each block
    for block in blocks:

        # Block must be a string that only contains digits
        if not block.isdigit():
            return False

        # Each integer must be between 0 and 255
        i = int(block)
        if i < 0 or i > 255:
            return False

    return True


@DataTypeManager.register("date-de")
def date_de(field):

    # German Date consists of three blocks
    blocks = field.split(".")

    if len(blocks) != 3:
        return False

    # Each block must be a number
    for block in blocks:
        if not block.isdigit():
            return False

    day, month, year = blocks

    return check_date(day, month, year)


@DataTypeManager.register("credit-card")
def credit_card(field):

    if not field.isdigit():
        return False

    sum = 0
    num_digits = len(field)
    oddeven = num_digits & 1

    for count in range(0, num_digits):
        digit = int(field[count])

        if not ((count & 1) ^ oddeven):
            digit = digit * 2
        if digit > 9:
            digit = digit - 9

        sum = sum + digit

    return (sum % 10) == 0


@DataTypeManager.register("date-iso8601")
def date_iso8601(field):

    # ISO-8601 Date consists of three blocks
    blocks = field.split("-")

    if len(blocks) != 3:
        return False

    # Each block must be a number
    for block in blocks:
        if not block.isdigit():
            return False

    year, month, day = blocks

    return check_date(day, month, year)


@DataTypeManager.register("aircraft-reg-us")
def aircraft_reg_us(field):

    # The number must be between 1 and 5 characters long (so with the "N", 2 to 6 characters)
    if not 2 <= len(field) <= 6:
        return False

    # The first character must be an "N"
    if field[0].lower() != "N":
        return False

    # The second character must be a digit other than zero
    if not field[1].isdigint():
        return False

    if int(field[1]) == 0:
        return False

    # The numbers must not contain the letters I or O
    for char in field.lower():
        if char == "i" or char == "o":
            return False

    return True


@DataTypeManager.register("time-24h")
def time_24h(field):

    # Length must be between 3 and 5 characters
    if not 3 <= len(field) <= 5:
        return False

    # The time consists of two blocks, separated by a ":"
    blocks = field.split(":")

    if len(blocks) != 2:
        return False

    hours, minutes = blocks

    # Hours must be digits between 0 and 23
    if not hours.isdigit():
        return False

    if not 0 <= int(hours) <= 23:
        return False

    # Minutes must be digits between 0 and 59
    if not minutes.isdigit():
        return False

    if not 0 <= int(minutes) <= 59:
        return False

    return True
