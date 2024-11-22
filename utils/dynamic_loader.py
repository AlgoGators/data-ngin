import importlib
import os
import yaml
from typing import Any, Dict


def load_config(config_path: str = "data/config/config.yaml") -> Dict[str, Any]:
    """
    Load configuration settings from a YAML file.

    Args:
        config_path (str): The path to the YAML configuration file.

    Returns:
        Dict[str, Any]: The loaded configuration as a dictionary.

    Raises:
        FileNotFoundError: If the specified configuration file does not exist.
        ValueError: If the configuration file is empty or invalid.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at {config_path}")

    with open(config_path, "r") as file:
        try:
            config: Dict[str, Any] = yaml.safe_load(file)
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing configuration file at {config_path}: {e}")

    if not config:
        raise ValueError(f"Configuration file at {config_path} is empty or invalid.")

    return config


def load_class(module_name: str, class_name: str) -> Any:
    """
    Dynamically load a class from a specified module.

    Args:
        module_name (str): The name of the module to import the class from.
        class_name (str): The name of the class to load.

    Returns:
        Any: The dynamically loaded class.

    Raises:
        ImportError: If the module cannot be imported or the class does not exist.
    """
    try:
        module: Any = importlib.import_module(module_name)
        cls: Any = getattr(module, class_name)
        return cls
    except ImportError as e:
        raise ImportError(f"Module '{module_name}' could not be imported: {e}")
    except AttributeError:
        raise ImportError(f"Class '{class_name}' does not exist in module '{module_name}'.")


def get_instance(config: Dict[str, Any], module_key: str, class_key: str, **kwargs: Any) -> Any:
    """
    Create an instance of a dynamically loaded class based on the configuration.

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
    # Validate the presence of module_key and class_key
    module_config = config.get(module_key)
    if module_config is None:
        raise ValueError(f"Module key '{module_key}' not found in configuration.")

    class_name: str = module_config.get(class_key)
    if class_name is None:
        raise ValueError(f"Class key '{class_key}' not found in '{module_key}' configuration.")

    # Extract module name and load class
    module_name: str = f"data.modules.{module_config.get('module', module_key)}"
    try:
        cls: Any = load_class(module_name, class_name)
        print(f"Successfully loaded class '{class_name}' from module '{module_name}'.")
    except ImportError as e:
        raise ImportError(f"Error loading class '{class_name}' from module '{module_name}': {e}")

    # Create and return an instance of the class
    return cls(config=config, **kwargs)
