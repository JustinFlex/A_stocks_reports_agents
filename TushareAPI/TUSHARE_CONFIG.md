# Tushare 本地配置说明

- Token 通过环境变量 `TUSHARE_API_KEY` 读取。请在 shell 中先导出，例如：`export TUSHARE_API_KEY='你的Tushare Token'`。
- 当前账号积分：5000（按需可在官网充值/升级）。
- 脚本示例：`example_tushare_call.py` 会用 `ts.pro_api(token)`（不落盘写文件）创建客户端并拉取一小段数据以验证 Token 可用。
- 运行环境：已安装 `tushare` 包（1.4.24）及依赖。

## 基础调用流程
1. 设置环境变量：`export TUSHARE_API_KEY='<你的token>'`
   - 若不想每次 export，可把这一行写入 `~/.bashrc` 或 `~/.profile`，然后 `source ~/.bashrc`。
2. 或者使用文件方式：在仓库根目录创建 `.tushare_token`（或 `~/.tushare_token`），文件内只放一行 token。
3. 统一使用代理：
   - 禁止修改 `/etc/wsl.conf` 或手动覆写 `/etc/resolv.conf`；保持 WSL 默认 DNS，复用宿主机网络。
   - 动态获取宿主机 IP：`host_ip=$(ip route | awk '/default/ {print $3; exit}')`
   - 设置代理：`export TUSHARE_PROXY="http://$host_ip:10808"`（端口按你的代理实际值修改）。可写入 `~/.bashrc` 持久化。
   - 不再提供直连/清理代理方案，所有外部访问必须走代理。

4. **端点说明（重点）**  
   - `tushare` 包默认指向 `http://api.waditu.com/dataapi`，近期容易 503 或返回空。  
   - 推荐统一用官方 `http://api.tushare.pro/dataapi`：  
     - 环境变量：`export TUSHARE_BASE_URL="http://api.tushare.pro/dataapi"`  
     - 或命令行：脚本加 `--base-url http://api.tushare.pro/dataapi`  
   - 示例脚本已支持该配置（`example_tushare_call.py` / 各周末效应脚本）。
4. 运行示例：`python3 example_tushare_call.py`
5. 预期输出：打印出交易日历的前几行以及总行数，表示 Token 调用正常。

## 关于财务接口的“重复行”
- 财务数据有时会出现看似重复的记录，通常是当季（或当年）财务数据被修订的结果。通过 `update_flag` 可辨别：`update_flag=1` 表示修订版，`update_flag=0` 表示初始披露。
- 若返回结果未包含 `update_flag`，请在查询中显式指定 `fields`，例如：`fields='ts_code,period,update_flag'`（字段以逗号分隔）。

## 网络说明：Windows vs WSL
- Windows：默认网络可用，若需要代理按常规方式设置即可；不要修改系统 DNS。
- WSL：必须复用宿主机网络与代理；不要动 `/etc/wsl.conf`、`/etc/resolv.conf`。设置 `TUSHARE_PROXY` 指向宿主机代理（IP 用 `ip route` 获取默认网关）。

## 安全与优先级
- Token 优先使用环境变量，其次 `.tushare_token` / `~/.tushare_token`（建议 `chmod 600`）。
- 代理优先级：显式 `TUSHARE_PROXY` > 现有环境变量；不使用直连方案。***
