from bmt import Toolkit

tk = None

def toolkit():
    global tk

    if tk is None:
        tk = Toolkit()

    return tk
