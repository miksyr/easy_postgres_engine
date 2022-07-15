from distutils.core import setup

setup(
    name="easy_postgres_engine",
    packages=["easy_postgres_engine"],
    version="0.2.4",
    description="Engine class for easier connections to postgres databases",
    author="Michael Doran",
    author_email="mikrdoran@gmail.com",
    url="https://github.com/miksyr/easy_postgres_engine",
    download_url="https://github.com/miksyr/easy_postgres_engine/archive/v_03.tar.gz",
    keywords=["postgreSQL", "postgres"],
    install_requires=["pandas>=1.3.2", "psycopg2-binary>=2.9.1", "testing.postgresql>=1.3.0", "numpy>=1.19.2"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
