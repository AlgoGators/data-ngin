import importlib
from typing import Any, Dict


def load_class(module_name: str, class_name: str) -> Any:
    """
    Dynamically load a class from a module.

    Args:
        module_name (str): The module name to import the class from.
        class_name (str): The class name to load.

    Returns:
        Any: The dynamically loaded class.
    """
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


def get_instance(config: Dict[str, Any], module_key: str, class_key: str, **kwargs) -> Any:
    """
    Create an instance of a dynamically loaded class based on configuration.

    Args:
        config (Dict[str, Any]): The configuration dictionary.
        module_key (str): The top-level key for the module in the configuration.
        class_key (str): The key for the class name in the module configuration.
        **kwargs: Additional arguments to pass to the class constructor.

    Returns:
        Any: An instance of the dynamically loaded class.

    Raises:
        ValueError: If the module_key or class_key is not found in the configuration.
    """
    if module_key not in config:
        raise ValueError(f"Module key '{module_key}' not found in configuration.")
    if class_key not in config[module_key]:
        raise ValueError(f"Class key '{class_key}' not found in '{module_key}' configuration.")

    # Extract module and class information
    module_name = f"data.modules.{config[module_key].get('module', module_key)}"
    class_name = config[module_key][class_key]

    # Dynamically load the class and create an instance
    cls = load_class(module_name, class_name)
    return cls(config=config, **kwargs)

