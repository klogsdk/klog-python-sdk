# 金山云日志服务(KLog) SDK for python使用指南

+ [金山云日志服务产品简介](https://www.ksyun.com/nv/product/KLog.html)
+ [金山云日志服务产品文档](https://docs.ksyun.com/products/123)

## 安装
有两种安装方式，通常情况下，使用在线安装即可。支持`python 2.7`和`python 3.x`。

### 在线安装
```shell
pip install klog-sdk
```

`pip install klog-sdk`会自动安装`lz4`和`protobuf`这两个依赖。如果自动安装依赖出现问题，可以尝试手动安装依赖：
```shell
pip install lz4
# python 3可使用protobuf的最新版本
# python 2.7可使用的protobuf版本最高为3.17.3
pip install protobuf==3.17.3
```

### 本地安装
```shell 
# 通过git下载SDK到本地
git clone https://gitee.com/klogsdk/klog-python-sdk.git

# 进入klog-python-sdk目录
cd klog-python-sdk

# 安装SDK
python setup.py install
```

## 使用方法
### 初始化KLog客户端
KLog客户端是线程安全的。在整个进程内您可以只创建一个KLog客户端，并重复使用。
```python
from klog import Client

# 您在金山云的主账户或子账户的 ACCESS KEY ID
access_key = "your secret_key"

# 您在金山云的主账户或子账户的 SECRET KEY ID
secret_key = "your secret_key"

# 您的日志项目所在地区的入口地址，该地址可以在金山云控制台日志服务的项目概览中查到。
# 支持 http 和 https
endpoint = "https://klog-cn-beijing.ksyun.com"

# 创建KLog客户端
client = Client(endpoint, access_key, secret_key)
```

### 上传文本类型日志
```python
# 异步发送一条文本日志
client.push("your_project_name", "your_pool_name", "your log message")
```

### 上传dict类型日志
KLog支持dict类型的日志。注意：同一日志池各条日志的dict数据结构应该保持一致。
```python
# 异步发送一条dict日志
client.push("your_project_name", "your_pool_name", {"k1": "value1", "k2": "value2"})
```

### 异步发送
KLog客户端默认是异步发送数据的，客户端内部的发送间隔为每2秒，或每批达到3MB，或每批达到4096条。 这样的好处有：
+ 客户端内部自动将最近的多条日志一起压缩并批量发送。
+ 不会阻塞其它逻辑(除非发送缓冲队列满了)
+ 可以配置各种发送策略

注意：
+ 程序退出时，需调用一次`Client.flush()`。

```python
# 立即发送客户端缓冲队列中还未发送的日志。
# 参数timeout可以为等待秒数或None。
# timeout为None时，表示阻塞到发送成功或发送结束为止。这种情况下的发送失败重试策略与Client的构造参数一致。
client.flush(timeout=10)
```

### 同步发送
+ 在调用`Client.push()`之后调用`Client.flush()`，可实现同步发送。

## 参数说明
### Client(endpoint, access_key, secret_key, queue_size=2000, drop_when_queue_is_full=False, rate_limit=0, down_sample_rate=1, max_retries=-1, retry_interval=-1, external_logger=None, logger_level=logging.WARNING)
创建一个KLog客户端，参数如下：
+ `endpoint` 必填。您的日志项目所在地区的入口地址，该地址可以在金山云控制台日志服务的项目概览中查到。支持`http`和`https`。
+ `access_key` 必填。您在金山云的主账户或子账户的`ACCESS KEY ID`。
+ `secret_key` 必填。您在金山云的主账户或子账户的`SECRET KEY ID`。
+ `queue_size` 客户端内部缓冲队列长度。默认为2000条日志。
+ `drop_when_queue_is_full` 默认情况下，当缓冲队列满时，`client.push()`会阻塞并等待空位。如果设置为`True`，则不等待，直接丢弃日志。默认为`False`
+ `rate_limit` 限制发送速率为每秒多少条。此项配置可降低CPU使用率，但会降低发送速率，在日志较多时，缓冲队列可能会满。默认为0，即不限制。
+ `down_sample_rate` 降采样率。例如设置为0.15时，将只发送15%的日志，其余丢弃。此项配置可降低CPU使用率。默认为1，即发送所有日志。
+ `max_retries` 发送失败后的重试次数，达到次数后如果仍然失败则丢弃日志。默认为-1，即永远重试。
+ `retry_interval` 发送失败后的重试间隔秒数。支持浮点数。默认为-1，即逐步增加重试间隔(但不会超过60秒)。
+ `external_logger` 设置客户端输出自身运行状态的日志对象。默认为None，即使用logging模块并打印到stdout。
+ `logger_level` 客户端内部日志打印level。默认为logging.WARNING。

### Client.push(project_name, log_pool_name, data, timestamp=None)
上传一条日志。参数如下：
+ `project_name` 必填。项目名称
+ `log_pool_name` 必填。日志池名称
+ `data` 必填。日志数据，字符串或dict类型。
+ `timestamp` 日志时间戳。默认取当前时间time.time()。

### Client.flush(timeout=None)
立即发送客户端缓冲队列中还未发送的日志。参数如下：
+ `timeout` 最大等待秒数，可以是小数。为None时，表示阻塞到发送成功或发送结束为止。这种情况下的发送失败重试策略与Client的构造参数一致。默认为None。
