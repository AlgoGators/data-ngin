import importlib
import os
import yaml
from typing import Any, Dict

def load_config(config_path: str = 'data\config\config.yaml') -> Dict[str, Any]:
    """
    Loads configuration settings from a YAML file.
    
    Returns:
        Dict[str, Any]: The loaded configuration as a dictionary.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at {config_path}")
    
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    return config


def load_class(module_name: str, class_name: str) -> Any:
    """
    Dynamically load a class from a module.

    Args:
        module_name (str): The name of the module to import the class from.
        class_name (str): The name of the class to load.

    Returns:
        Any: The dynamically loaded class.

    Raises:
        ImportError: If the module or class cannot be loaded.
    """
    try:
        module: Any = importlib.import_module(module_name)
        cls: Any = getattr(module, class_name)
        return cls
    except (ImportError, AttributeError) as e:
        raise ImportError(f"Could not load class '{class_name}' from module '{module_name}': {e}")


def get_instance(config: Dict[str, Any], module_key: str, class_key: str, **kwargs: Any) -> Any:
    """
    Create an instance of a dynamically loaded class based on configuration.

    Args:
        config (Dict[str, Any]): The configuration dictionary.
        module_key (str): The top-level key for the module in the configuration.
        class_key (str): The key for the class name in the module configuration.
        **kwargs (Any): Additional arguments to pass to the class constructor.

    Returns:
        Any: An instance of the dynamically loaded class.

    Raises:
        ValueError: If the module_key or class_key is not found in the configuration.
        ImportError: If the module or class cannot be loaded.
    """
    print(f"get_instance called with module_key={module_key}, class_key={class_key}, kwargs={kwargs}")
    if module_key not in config:
        raise ValueError(f"Module key '{module_key}' not found in configuration.")
    if class_key not in config[module_key]:
        raise ValueError(f"Class key '{class_key}' not found in '{module_key}' configuration.")

    # Extract module and class information
    module_name: str = f"data.modules.{config[module_key].get('module', module_key)}"
    class_name: str = config[module_key][class_key]

    # Dynamically load the class and create an instance
    cls: Any = load_class(module_name, class_name)
    return cls(config=config, **kwargs)
