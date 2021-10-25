try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

with open('README.md', 'rb') as f:
    readme = f.read().decode('utf-8')

setup(
    name="klog-sdk",
    version="0.8",
    description="Kingsoft Log Service SDK",
    long_description=readme,
    long_description_content_type="text/markdown",
    packages=[
        "klog",
        "klog.protobuf"
    ],
    install_requires=[
        "protobuf==3.17.3",
        "lz4"
    ],
    include_package_data=True,
    url="https://gitee.com/klogsdk/klog-python-sdk",
    author="KLog Developers",
    author_email="ksc-klog@kingsoft.com",
    license="MIT",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3"
    ],
    python_requires=">=2.7",
    keywords='klog, klog-sdk, kingsoft-cloud',
)
