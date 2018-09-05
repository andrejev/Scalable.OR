from scalableor.manager import DataTypeManager


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
