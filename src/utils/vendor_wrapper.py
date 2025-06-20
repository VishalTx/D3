import importlib

from utils.config_wrapper import ConfigWrapper


class VendorWrapper(ConfigWrapper):
    config = None
    def __init__(self):
        super().__init__()

    def load_configs(self, vendor_name, asset_name):
        module_path = f"configs.{vendor_name}.{asset_name}"
        try:
            module = importlib.import_module(module_path)
            self.config = module.config  # Assuming each config file has a `config()` function
        except ModuleNotFoundError as e:
            raise ImportError(f"Config module not found: {module_path}") from e
        except AttributeError:
            raise ImportError(f"Config function not found in module: {module_path}")
