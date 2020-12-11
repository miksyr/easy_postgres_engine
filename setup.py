from distutils.core import setup

setup(
    name='easy_postgres_engine',
    packages=['easy_postgres_engine'],
    version='0.1',
    description='Engine class for easier connections to postgres databases',
    author='Michael Doran',
    author_email='mikrdoran@gmail.com',
    url='https://github.com/miksyr/easy_postgres_engine',
    download_url='https://github.com/miksyr/easy_postgres_engine/archive/v_01.tar.gz',
    keywords=['postgreSQL', 'postgres'],
    install_requires=[
            'pandas==0.25.3',
            'psycopg2-binary==2.8.4',
            'testing.postgresql==1.3.0'
      ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
)
