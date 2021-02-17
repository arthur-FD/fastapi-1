# import os.path
# import sys

# PARENT_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# found_parent_dir=False

# for p in sys.path:
#     if os.path.abspath(p)==PARENT_DIR:
#         found_parent_dir=True
#         break
# if not found_parent_dir:
#     sys.path.insert(0,PARENT_DIR)


try:
    import pkg_resources
    pkg_resources.declare_namespace(__name__)
except ImportError:
    import pkgutil
    __path__ = pkgutil.extend_path(__path__,__name__)