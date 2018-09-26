import re

pattern = re.compile("\$\{(.*?)\}")


def apply_context(data, context):
    """
    :param data: Python data from JSON.
    :param context: list of dictionaries for parameter lookup
    :return: The input data structure where '${param_name}' in Unicode in lists
        and dicts are replaced with the first value from the context.
    """
    def subst(matchobj):
        key = matchobj.group(1)
        return context[key]
    if isinstance(data, list):
        return [apply_context(value, context) for value in data]
    elif isinstance(data, dict):
        return {key: apply_context(value, context) for key, value in data.items()}
    elif isinstance(data, unicode):
        return pattern.sub(subst, data)
    else:
        return data


class Context:
    def __init__(self, env_list=[]):
        if isinstance(env_list, dict):
            env_list = [env_list]
        self.env_list = env_list

    def __getitem__(self, key):
        for env in self.env_list:
            value = env.get(key)
            if value is not None:
                return value
        raise TemplateError("No value for template variable {}".format(key))

    def has_key(self, key):
        for env in self.env_list:
            value = env.has_key(key)
            if value:
                return True
        return False

    def extend(self, env):
        return Context([env] + self.env_list)


class TemplateError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)
