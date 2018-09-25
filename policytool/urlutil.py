

# TODO: Validate incoming URL.

def get_host(url):
    return url.split('/')[2]


def get_path(url):
    if url is None:
        return None
    return "/" + "/".join(url.split('/')[3:])
