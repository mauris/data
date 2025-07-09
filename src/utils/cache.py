import json
cacheFile = "cache.json"
isCacheEnabled = True

def load_from_file():
    """
    Load cached data from a JSON file.
    Returns the cached data as a dictionary.
    """
    if not isCacheEnabled:
        return {}
    try:
        with open(cacheFile, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        print("Cache file is corrupted or empty.")
        return {}

def save_to_file(cache):
    """
    Save the cache dictionary to a JSON file.
    """
    if not isCacheEnabled:
        return
    try:
        with open(cacheFile, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except IOError as e:
        print(f"Error saving cache to file: {e}")

c = load_from_file()

def save_cache():
    """
    Save the current cache to the file.
    This is a convenience function to ensure the cache is saved.
    """
    if not isCacheEnabled:
        return
    save_to_file(c)

def cache(func):
    """
    Decorator to cache the results of a function.
    The cache is stored in a dictionary with the function's name as the key.
    """

    def wrapper(*args, **kwargs):
        key = f"{func.__name__}/{json.dumps(args)}/{json.dumps(kwargs)}"
        if not isCacheEnabled or key not in c:
            c[key] = func(*args, **kwargs)
        return c[key]

    return wrapper