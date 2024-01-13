from setuptools import setup, find_packages

setup(
    name="ouro",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "supabase",
        "python-dotenv",
        "postgrest",
    ],
    python_requires=">=3.6",
    author="Matt Moderwell",
    author_email="matt@ouro.com",
    description="Python wrapper for the Ouro API",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/ouro/ouro_py",
    classifiers=[
        # Classifiers help users find your project based on certain keywords
        # Example: 'Development Status :: 4 - Beta'
        # Full list at https://pypi.org/classifiers/
    ],
    keywords="package keywords",  # Replace with keywords relevant to your package
    license="MIT",  # Replace with your chosen license
)
