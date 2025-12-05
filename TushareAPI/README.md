# Tushare API 本地资源库

> 目的：为自动化/Agent 使用 Tushare API 时提供现成的文档缓存、示例代码和网络配置指引，开箱即用。

## 目录与用途
- `tushare_api_docs/`：按官网目录结构抓取的 API 文档，最细颗粒度分目录/文件，每个接口一个 txt，文件名包含中文名和 `doc_id`。
- `fetch_tushare_api_docs.py`：爬取 `https://tushare.pro/document/2` 左侧导航的所有接口文档并分类保存到 `tushare_api_docs/`。
- `example_tushare_call.py`：最小化 API 调用示例，读取本地 Token，调用交易日历接口验证连通性。
- `TUSHARE_CONFIG.md`：Token 获取方式、网络/代理说明、WSL 特殊配置（DNS、代理开关）、示例运行步骤。

## 快速开始
> 代理需保持 **Whitelist** 模式；Global 模式会导致 Tushare TLS 握手失败。

1. 准备 Token：在环境变量 `TUSHARE_API_KEY` 导出，或在仓库根目录/家目录放 `.tushare_token`（文件内一行 Token）。
2. 代理设置（Whitelist）：
   - 获取宿主机网关 IP：`host_ip=$(ip route | awk '/default/ {print $3; exit}')`
   - 设置代理：`export TUSHARE_PROXY=\"http://$host_ip:10808\"`（端口按实际代理修改，可写入 `~/.bashrc` 持久化）
3. 验证/探测：
   ```bash
   # 默认走代理，调用交易日历
   python3 example_tushare_call.py --start 20240101 --end 20240110

   # 仅做网络探测（不发正式查询）
   python3 example_tushare_call.py --probe
   ```
4. 刷新文档：
   ```bash
   python3 fetch_tushare_api_docs.py
   ```
   生成/更新 `tushare_api_docs/` 下的分类接口文档。
5. 周五效应分析：
   ```bash
   # 潍柴动力周一/周五效应 + 周五买入策略
   python3 weichai_weekend_effect_tushare.py

   # 按年度输出检验
   python3 weichai_weekend_effect_tushare.py --yearly
   ```

## 财务接口重复行的处理
- 若发现财务数据出现重复行，大多是当期数据被修订导致。通过 `update_flag` 字段判断：`update_flag=1` 为修订版，`update_flag=0` 为初始披露。
- 如果响应未携带 `update_flag`，在查询时显式指定 `fields`，例如：`fields='ts_code,period,update_flag'`（字段用逗号分隔）。

## WSL 特殊说明
- 不要修改 `/etc/wsl.conf`、`/etc/resolv.conf`，保持默认 DNS，复用宿主机网络。
- 通过 `host_ip=$(ip route | awk '/default/ {print $3; exit}')` 获取宿主机网关 IP，设置 `TUSHARE_PROXY` 指向宿主机代理（示例端口 10808 按实际修改）。

## 其他
- 安全：示例和脚本优先从环境变量读取 Token；也支持 `.tushare_token`（建议 `chmod 600`）。
- 依赖：已安装 `tushare` (1.4.24) 及其依赖。无需额外服务。***
