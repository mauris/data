from processors.hawker_center_closure import hawker_center_closure
from utils.cache import save_cache

if __name__ == "__main__":
    hawker_center_closure()
    save_cache()  # Ensure cache is saved after processing