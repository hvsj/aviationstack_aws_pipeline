from pathlib import Path

CURRENT_DIR = Path(__file__).parent if "__file__" in locals() else Path.cwd()

print(type(CURRENT_DIR))
print(type(str(CURRENT_DIR)))
print(str(CURRENT_DIR))
