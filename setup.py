from setuptools import setup, find_packages

setup(
    name="ouro",
    # version="0.1",
    packages=find_packages(),
    install_requires=[
        "supabase",
        "python-dotenv",
        "postgrest",
        "pandas",
        "numpy",
        "httpx",
        "anyio",
        "pydantic",
        "distro",
    ],
    python_requires=">=3.7",
    author="Matt Moderwell",
    author_email="matt@ouro.foundation",
    description="The official Python library for the Ouro API",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/ourofoundation/ouro-py",
    classifiers=[],
    keywords="ouro",
    license="MIT",
)
