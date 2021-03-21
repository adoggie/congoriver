#coding:utf-8


from setuptools import setup,find_packages
import congoriver
setup(
    name='elabs-congoriver',
    version=congoriver.VERSION,
    keywords='elabs',
    description='python 2.x/3.x 2021/3/15 ',
    long_description="",
    author='zhangbin',
    author_email='zhangbin@evolutionlabs.com.cn',
    url='http://www.evolutionlabs.com.cn',
    # packages=find_packages(),
    packages=['congoriver'],
    license='GNU',
    data_files = ['congoriver/config.yaml',],
    install_requires = ['fire','PyYAML','pyzmq'],
    include_package_data = True
      )


"""
python setup.py build 
python setup.py sdist 

"""