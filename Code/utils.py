def check_range(_value, minv, maxv = None):
    if maxv == None:
        _maxv = 'inf'
    else:
        _maxv = str(maxv)

    try:
        value = float(_value)
    except ValueError:
        raise argparse.ArgumentTypeError("Value should be in [" + str(minv) + ", " + _maxv + "]")

    if value < minv or (maxv != None and value > maxv):
        raise argparse.ArgumentTypeError("Value should be in [" + str(minv) + ", " + _maxv + "]")
    return value

def check_01(value):
    return check_range(value, 0, 1)

def check_int(value, minv, maxv = None):
    try:
        if int(value) != float(value):
            raise argparse.ArgumentTypeError("Value should be an integer")
    except ValueError:
            raise argparse.ArgumentTypeError("Value should be an integer")
    return int(check_range(value, minv, maxv))

def check_nonneg(value):
    return check_int(value, 1)

def check_pos(value):
    return check_int(value, 0)

