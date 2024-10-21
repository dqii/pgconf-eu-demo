import os

# Define the output Markdown file
output_file = "temp.md"

# Get all .py files in the current directory
py_files = [f for f in os.listdir('.') if f.endswith('.py')]

# Open the output file in write mode
with open(output_file, 'w') as md_file:
    # Iterate through the Python files
    for py_file in py_files:
        # Write the file name as a markdown header
        md_file.write(f"# {py_file}\n\n")
        # Write the content of the Python file
        with open(py_file, 'r') as f:
            contents = f.read()
            md_file.write("```\n")
            md_file.write(contents)
            md_file.write("\n```\n\n")

print(
    f"Markdown file '{output_file}' has been created with the contents of all .py files.")
