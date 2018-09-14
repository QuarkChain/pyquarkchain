def colorify(s, color):
    color_map = {
        "grey": "\033[90m",
        "red": "\033[91m",
        "green": "\033[92m",
        "yellow": "\033[93m",
        "purple": "\033[94m",
        "pink": "\033[95m",
        "blue": "\033[96m",
    }

    return color_map[color] + s + "\033[0m"


def underline(s):
    return "\033[4m" + s + "\033[0m"
