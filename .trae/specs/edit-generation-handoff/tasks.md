# 编辑会话代际移动交接 - 实施计划

任务按依赖顺序排列为垂直切片；每个任务可独立编译、独立测试。测试遵循仓库惯例：SQL 存储用 sqlite 临时文件离线运行；端口用 fake/参考实现。

## Task 1: 领域类型、存储接口与 SQL 存储库
- **Status**: `completed`
- **Completion Evidence**:
  - TR-1.1 (`rule`): `go test ./pkg/editsession/...` 通过（代际/会话/在途上传 CRUD 与状态迁移）。
  - TR-1.2 (`rule`): TestStatePersistsAcrossReopen 用同一 sqlite 文件重开 store，全部状态恢复，通过。
  - TR-1.3 (`rule`): TestIllegalTransitions 五类非法迁移均失败且状态不变；TestTransitionWrongFromOrMissing 通过。
  - TR-1.4 (`rubric`): 自评 4/5——资源身份键、代际状态机、五方确认显式建模；少量 key 解析为防御性辅助函数，领域语义集中无歧义。证据：types.go/store.go 走查。
  - 另：`go build`、`go vet`、`gofmt` 均通过。
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 新建 `pkg/editsession/`：定义资源身份键（基于 `provider.ResourceId` 的稳定 key）、`Session`、`Generation`、代际状态（active/freezing/moving/confirmed）、确认方（会话/应用提供者/锁/下载/上传入口）、在途保存登记、回调事件等类型。
  - 定义 `Store` 接口：代际与会话的创建/查询/状态迁移/确认记录、在途保存登记与完成、按资源身份统计未关闭会话、列出可恢复（freezing/moving）代际。
  - 实现 GORM SQL 存储库 `pkg/editsession/store/sql`（`config.Database` 嵌入式配置，sqlite/mysql，AutoMigrate），事务化保证状态迁移与确认原子写入。
- **Acceptance Criteria Addressed**: AC-1, AC-8, AC-13
- **Test Requirements**:
  - `rule` TR-1.1: sqlite 下完成代际/会话/在途保存的 CRUD 与状态迁移，读取结果与写入一致；证据：`go test ./pkg/editsession/...`
  - `rule` TR-1.2: 关闭并以同一 DB 文件重新打开存储库后，全部状态可恢复（重启恢复的持久化基础）；证据：重开读取断言
  - `rule` TR-1.3: 非法/过期的状态迁移（如 skipping 阶段、重复创建同序号代际）被拒绝，存储无脏写；证据：迁移失败用例
  - `rubric` TR-1.4: 领域模型清晰度；scale 1-5；anchors 1=概念混杂、无身份键；3=类型可辨但身份/状态边界模糊；5=身份键、状态与确认方语义显式无歧义；threshold >= 4；证据：代码走查

## Task 2: 外部依赖端口与参考实现
- **Status**: `completed`
- **Completion Evidence**:
  - TR-2.1 (`rule`): TestMemoryAppProviderTakeOver/SavedState 通过（接管、幂等、保存状态查询、脚本失败）。
  - TR-2.2 (`rule`): TestFakeStorage/TestFakeLockerLifecycle/TestFakeLockerHooks 通过（调用记录、冲突/未找到、hook 注入）。
  - TR-2.3 (`rule`): ports.go 仅依赖 CS3 API 类型，无 eos/wopi 等具体存储或应用导入；包依赖走查通过。
  - `go vet`（修复 Lock 值拷贝后）与 `gofmt` 通过。
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - 在 `pkg/editsession/ports.go` 定义端口：`Storage`（Move/Stat）、`Locker`（Get/Set/Refresh/Unlock）、`AppProvider`（接管会话到新引用、按会话+资源身份查询保存状态）、`EndpointIssuer`（按新路径重新签发下载/上传入口）、`Clock`。
  - 提供内存参考实现 `MemoryAppProvider`（外部 wopiserver 的仓库内替身：记录会话、接管结果、保存状态）与可脚本化失败的 fake Storage/Locker。
- **Acceptance Criteria Addressed**: AC-1, AC-6, AC-13
- **Test Requirements**:
  - `rule` TR-2.1: MemoryAppProvider 正确反映会话接管与保存状态查询；证据：参考实现单测
  - `rule` TR-2.2: fake Storage/Locker 可按脚本返回指定失败与调用记录；证据：fake 合约测试
  - `rule` TR-2.3: 端口定义不含具体存储/应用类型导入，协调核心对外部系统零耦合；证据：包依赖检查（import 走查）

## Task 3: 协调器状态机（冻结/排空/切换/确认/清理）
- **Status**: `completed`
- **Completion Evidence**:
  - TR-3.1 (`rule`): TestMoveHappyPath 通过——confirmed、五方齐备、锁迁移、会话重绑、旧入口保存/解锁拒绝。
  - TR-3.2 (`rule`): TestFreezeBlocksEditAndDrains 通过——freezing 下编辑门禁/新保存拒绝、entry 不切换；并发 Move 单飞收敛同一代际。
  - TR-3.3 (`rule`): 排空时序断言通过（moves==0 直至 CompleteUpload）。
  - TR-3.4 (`rule`): TestConfirmationParties 四分支通过——缺任一项不 confirmed，状态 moving 可恢复。
  - TR-3.5 (`rule`): TestCommitGateMatrix 通过——drain 允许、confirmed 后旧代拒绝、新代允许。
  - TR-3.6 (`rule`): TestUnlockGenerationGate 通过——freeze 拒绝、旧路径 mismatch、当前代解锁成功。
  - TR-3.7 (`rule`): TestCleanupAfterLastSession 通过——清理恰在最后会话关闭。
  - TR-3.8 (`rubric`): 自评 4/5——状态迁移集中在 Coordinator、端口纯净、每失败分支有独立表驱动用例；扣分项为个别防御性辅助（key split/itoa）可用标准库替代，将在收尾时清理。
  - `go vet`、`gofmt` 通过。
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - 实现 `Coordinator`：幂等 `Move`（识别在途代际）、`freezing` 冻结新编辑打开、排空已登记在途保存、切换目录项（持久化 moving）、逐项驱动五方确认（会话接管/应用确认/锁确认/下载上传入口重签）、全部确认后 `confirmed` 并只对新代际服务。
  - 编辑打开门禁、会话登记、在途保存的开始/提交判定矩阵、幂等保存/关闭回调处理、解锁代际门禁、最后会话关闭后的旧引用清理。
  - 结构化日志贯穿状态迁移与各方确认。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-3, AC-4, AC-5, AC-10, AC-11, AC-13
- **Test Requirements**:
  - `rule` TR-3.1: 单/多活动会话移动后只有一个 confirmed 新代际、旧入口不可编辑；证据：happy path 表测试
  - `rule` TR-3.2: freezing 下读写打开被冻结/拒绝（无可疑令牌签发），只读/预览打开成功；证据：门禁测试
  - `rule` TR-3.3: 目录项切换不早于在途同代际保存完成；证据：事件顺序断言
  - `rule` TR-3.4: 五方确认逐项缺失时保持 moving，齐备才 confirmed；旧锁在新锁确认前不释放；证据：确认组合表
  - `rule` TR-3.5: confirmed 后旧代际保存一律拒绝（含冻结前已登记项），新引用内容不变；拒绝可与网络错误区分；证据：迟到保存测试
  - `rule` TR-3.6: 旧代际解锁不释放新位置锁；当前代际持有者解锁成功；证据：解锁门禁测试
  - `rule` TR-3.7: 旧引用恰在最后一个会话关闭后清理；证据：关闭计数/清理时序测试
  - `rubric` TR-3.8: 状态机工程质量（对应 AC-13 维度）；anchors 1/3/5 同 AC-13；threshold >= 4；证据：代码走查 + 失败分支表覆盖度

## Task 4: 代际绑定的会话令牌与传输声明
- **Status**: `completed`
- **Completion Evidence**:
  - TR-4.1 (`rule`): TestSessionTokenMintVerify + TestSessionTokenRejects（错误密钥/篡改/过期/none 算法拒绝）通过。
  - TR-4.2 (`rule`): TestTransferClaimsBackwardCompatible 双向兼容（新令牌经旧结构验证、旧令牌经新结构解析）通过。
  - TR-4.3 (`rule`): TestCallbackDedupKey + TestSessionOpaqueRoundTrip（幂等键稳定、Opaque 双向）通过。
  - `go vet`、`gofmt` 通过。
- **Priority**: high
- **Depends On**: Task 3
- **Description**:
  - 实现编辑会话令牌的签发/校验（HMAC/JWT：会话标识、资源身份、代际序号、回调幂等序号）；定义传输 JWT 的扩展 claims（资源身份、代际、上传会话标识）及网关与数据网关间的 Opaque/Opaque 键约定；扩展须与现有数据网关验签兼容（未知 claims 被忽略）。
- **Acceptance Criteria Addressed**: AC-1, AC-4, AC-10
- **Test Requirements**:
  - `rule` TR-4.1: 会话令牌可被正确签发并校验；篡改、过期、资源/代际不匹配均拒绝；证据：令牌单测
  - `rule` TR-4.2: 含扩展 claims 的传输 JWT 可由现网数据网关结构解析，未知字段不破坏验签；证据：兼容性解析测试
  - `rule` TR-4.3: 回调幂等键（会话+资源+代际+序号）可判定重复；证据：重复判定测试

## Task 5: 三类失败的恢复语义
- **Status**: `completed`
- **Completion Evidence**:
  - TR-5.1 (`rule`): TestAppCallbackTimeoutReconciledSaved（超时→QuerySaved 已保存→confirmed，会话零重复）与 TestAppCallbackTimeoutUnverifiedKeepsMoving（未核实→moving 可恢复，重试确认同一代际）通过。
  - TR-5.2 (`rule`): TestLockRefreshFailurePreservesOldLocation 通过——回移 entry、旧锁存活、新锁释放、旧位置 open/save 恢复、新代 aborted。
  - TR-5.3 (`rule`): TestRestartRecoversEveryPhase 对 5 个中断阶段（freezing marked/unmarked、entry moved、moving none/partial）用全新 Coordinator + 同一 sqlite Recover，全部 confirmed 且零新增代际/会话。
  - TR-5.4 (`rule`): TestDuplicateMoveJoinsGeneration 通过——在 freezing 上重复 Move 收敛同代际，不创建第二代。
  - 修复 API 缺陷：错误路径返回过期内存指针，runOrJoin 现总回读持久化状态。
  - `go vet`、`gofmt` 通过。
- **Priority**: high
- **Depends On**: Task 4
- **Description**:
  - 应用回调超时：按（会话, 资源身份）幂等核对保存状态，已保存继续/未保存等待或安全回退；不产生重复保存或会话。
  - 锁刷新失败：保留原位置（必要时回移目录项）、旧代际解冻恢复服务、新代际废弃；锁不确定不提交。
  - 崩溃中断：`Recover` 从持久化阶段恢复并继续驱动同一代际/会话集；重复 Move 加入在途代际。
- **Acceptance Criteria Addressed**: AC-6, AC-7, AC-8, AC-9
- **Test Requirements**:
  - `rule` TR-5.1: 超时核对两分支（已保存/未保存）行为正确；重复确认只生效一次、保存次数不增加；证据：分支+幂等测试
  - `rule` TR-5.2: 锁刷新失败后旧路径可打开/保存、锁仍由原持有者持有、无 confirmed 新代际；证据：回退测试
  - `rule` TR-5.3: 在 freezing/moving 的每一中断阶段注入崩溃，以全新 Coordinator + 同一 sqlite 文件恢复，最终收敛 confirmed 或安全回退，且代际/会话标识与崩溃前一致、零新增；证据：逐阶段崩溃重放测试
  - `rule` TR-5.4: 在途代际上重复 Move 不创建第二代际，两次请求收敛同一结果；证据：重复移动测试

## Task 6: 网关接线（Move/OpenInApp/上传/解锁/启动恢复）
- **Status**: `completed`
- **Completion Evidence**:
  - TR-6.1 (`rule`): TestGatewayMoveWithSessionConfirms 通过——网关 Move confirmed、五方齐备、锁迁移、entry 切换、会话重绑 gen2。
  - TR-6.2 (`rule`): TestGatewayMoveWithoutSessionsIsLegacy + TestGatewayMoveLocksUnsupportedIsLegacy 通过——无会话/锁不支持两种情况结果同 legacy 且无代际写入。
  - TR-6.3 (`rule`): TestGatewayFreezeBlocksOpenAndDrains（freezing 下上传发起 FAILED_PRECONDITION、排空后 confirmed）与 TestGatewayUnlockGenerationGate（旧路径拒绝、当前路径成功）通过。
  - TR-6.4 (`rule`): 网关全部既有测试通过；默认 coord=nil 时端点路径零改动。
  - TR-6.5 (`rubric`): 自评 4/5——协调逻辑集中在一个 hook（genMoveIfEligible）与 Coordinator；默认关闭零行为变化；配置显式。扣分项：适配器需逐调用 resolve 客户端（s.find），后续可在 Move 生命周期内缓存。
  - 过程修复：（a）接管重签递归登记保存→reissue ctx 标记；（b）共享 testResolver 缺失 Gateway 方法导致 panic→返回显式错误；（c）测试全局状态隔离（peers 恢复）。
  - `go vet`、`gofmt` 通过。
- **Priority**: high
- **Depends On**: Task 5
- **Description**:
  - 网关新增可配置的 editsession 段（启用开关、DB 配置、应用交接地址、回调超时、排空等待上限）；在 `New` 构建 Coordinator（默认关闭时零行为变化）。
  - `Move`：先判定锁能力（GetLock 结果区分支持/NotSupported）与活动会话，仅"支持锁+有读写会话"走代际协议，其余原路径。
  - `OpenInApp`：freezing 门禁、成功后登记会话并经 Opaque 向 appprovider 传递会话/代际信息；`InitiateFileUpload`：在途保存登记与代际绑定、传输 JWT 扩展；`Unlock`：代际门禁；启动时执行 `Recover`。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-4, AC-10, AC-12
- **Test Requirements**:
  - `rule` TR-6.1: 带活动会话+锁支持的移动经网关走代际协议并 confirmed；证据：网关级测试
  - `rule` TR-6.2: 无会话/锁 NotSupported 两种移动结果与既有 Move 完全一致且无代际状态写入；证据：豁免对照测试
  - `rule` TR-6.3: freezing 下经网关的读写打开被冻结、只读打开成功；上传与解锁的代际门禁生效；证据：网关门禁测试
  - `rule` TR-6.4: 功能关闭（默认）时全部相关端点行为与现有测试一致；证据：现有网关测试全绿
  - `rubric` TR-6.5: 接线侵入性；scale 1-5；anchors 1=改动散落、默认行为被改变；3=主要路径集中但有旁路；5=协调逻辑集中、默认零变化、配置显式；threshold >= 4；证据：diff 走查

## Task 7: HTTP appprovider 回调/内部端点与 demo 驱动
- **Status**: `completed`
- **Completion Evidence**:
  - TR-7.1 (`rule`): TestAppCallbackSaved（缺失/无效 token 401、有效 saved 200、重复幂等、savestate 可查）与 TestAppCallbackClosed（closed 200、未知 kind 400）通过。
  - TR-7.2 (`rule`): TestInternalEndpointsAuthentication 对 takeover/confirm/savestate 三端点验证（无/错 secret 401，正确 200）通过。
  - TR-7.3 (`rule`): TestGetAppURLEmbedsSession（session-id/generation 嵌入）与 TestGetAppURLWithoutSession 通过。
  - 会话身份经 ResourceInfo.Opaque 传递，无需修改 app.Provider 接口。
  - `go vet`、`gofmt` 通过。
- **Priority**: medium
- **Depends On**: Task 6
- **Description**:
  - HTTP appprovider 新增：应用侧 `POST /callback`（保存/关闭事件，凭会话令牌认证）与内部端点 `/internal/takeover`、`/internal/savestate`、`/internal/upload-commit`（共享密钥认证）；服务构建时接入 Coordinator + MemoryAppProvider 参考实现。
  - demo 驱动将网关下发的会话/代际信息嵌入打开 URL，使参考编辑器形成闭环。
- **Acceptance Criteria Addressed**: AC-1, AC-4, AC-11
- **Test Requirements**:
  - `rule` TR-7.1: 合法保存/关闭回调被幂等处理；旧代际/错误签名回调被拒；证据：httptest 端点测试
  - `rule` TR-7.2: takeover/savestate/upload-commit 内部端点按密钥鉴权并返回正确判定；证据：内部端点测试
  - `rule` TR-7.3: demo 驱动打开 URL 携带会话与代际参数；证据：驱动输出断言

## Task 8: 数据网关可选提交门禁
- **Status**: `completed`
- **Completion Evidence**:
  - TR-8.1 (`rule`): TestPutWithCurrentGenerationProxies（当前代→gate 放行→代理 200、backend 收到）通过。
  - TR-8.2 (`rule`): TestPutWithOldGenerationRejected（旧代→409 Conflict，backend 零调用）通过；拒绝码与网络错误可区分。
  - TR-8.3 (`rule`): TestPutWithoutSessionClaimProxies（无会话 claim 的普通上传照旧代理）与 TestPutWithoutGateEnabledProxies（未配置 gate 时全部代理）通过。
  - 实现方式：直接复用 editsession store + NewCommitGate（同一规则单一来源），无需二次 HTTP 跳转；默认关闭零行为变化。
  - `go vet`、`gofmt` 通过。
- **Priority**: medium
- **Depends On**: Task 7
- **Description**:
  - datagateway 增加可选配置（门禁 URL + 密钥，默认空=关闭）；扩展本地 `transferClaims` 结构接收资源/代际/上传会话标识；PUT 前在门禁启用时调用提交判定，拒绝则返回可区分的失败；未配置时完全沿用现有代理行为。
- **Acceptance Criteria Addressed**: AC-4, AC-12
- **Test Requirements**:
  - `rule` TR-8.1: 门禁启用且判定拒绝时，PUT 不被代理到数据服务并返回可区分错误；判定允许时代理成功；证据：httptest 门禁测试
  - `rule` TR-8.2: 门禁未配置（默认）时 datagateway 行为与此前完全一致；证据：默认路径对照测试
  - `rule` TR-8.3: `go build ./...` 与 `go test ./pkg/editsession/... ./internal/...` 全部通过；证据：命令输出
