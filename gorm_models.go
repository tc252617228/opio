package opio

import "time"

// ====================================================================================
// GORM Data Models (Moved from models.go - Potentially for Metadata or Future Use)
// ====================================================================================

// GormPoint 对应数据库中的 'point' 表 (点表) - 保留用于可能的元数据查询
type GormPoint struct {
	ID int32     `gorm:"column:ID;primaryKey"`     // 点标识
	UD int64     `gorm:"column:UD"`                // UUID
	ND int32     `gorm:"column:ND"`                // 父节点标识
	PT int8      `gorm:"column:PT"`                // 点的来源
	RT int8      `gorm:"column:RT"`                // 点的类型
	PN string    `gorm:"column:PN;type:char(32)"`  // 点名
	AN string    `gorm:"column:AN;type:char(32)"`  // 别名
	ED string    `gorm:"column:ED;type:char(60)"`  // 描述
	KR string    `gorm:"column:KR;type:char(16)"`  // 特征字
	SG []byte    `gorm:"column:SG;type:binary(4)"` // 安全组
	FQ int16     `gorm:"column:FQ"`                // 分辨率
	CP int16     `gorm:"column:CP"`                // 处理器
	HW int32     `gorm:"column:HW"`                // 模块地址
	BP int16     `gorm:"column:BP"`                // 通道号
	LC int8      `gorm:"column:LC"`                // 报警类型
	AP int8      `gorm:"column:AP"`                // 报警优先级
	AR int8      `gorm:"column:AR"`                // 存档
	FL int32     `gorm:"column:FL"`                // 标志位
	ST string    `gorm:"column:ST;type:char(6)"`   // 值为 1 时的描述
	RS string    `gorm:"column:RS;type:char(6)"`   // 值为 0 时的描述
	EU string    `gorm:"column:EU;type:char(12)"`  // 单位
	FM int16     `gorm:"column:FM"`                // 显示小数位
	IV float32   `gorm:"column:IV"`                // 初始值
	TV float32   `gorm:"column:TV"`                // 量程上限
	BV float32   `gorm:"column:BV"`                // 量程下限
	LL float32   `gorm:"column:LL"`                // 报警低限
	HL float32   `gorm:"column:HL"`                // 报警高限
	ZL float32   `gorm:"column:ZL"`                // 报警低 2 限
	ZH float32   `gorm:"column:ZH"`                // 报警高 2 限
	L3 float32   `gorm:"column:L3"`                // 报警低 3 限
	H3 float32   `gorm:"column:H3"`                // 报警高 3 限
	L4 float32   `gorm:"column:L4"`                // 报警低 4 限
	H4 float32   `gorm:"column:H4"`                // 报警高 4 限
	DB float32   `gorm:"column:DB"`                // 死区
	DT int8      `gorm:"column:DT"`                // 死区类型
	KZ int8      `gorm:"column:KZ"`                // 压缩类型
	KT int8      `gorm:"column:KT"`                // 计算类型
	KO int8      `gorm:"column:KO"`                // 计算顺序
	CT time.Time `gorm:"column:CT"`                // 修改时间
	EX string    `gorm:"column:EX"`                // 计算表达式
	GN string    `gorm:"column:GN"`                // 全局名称
}

// TableName 指定 GormPoint 结构体对应的数据库表名
func (GormPoint) TableName() string {
	return "point"
}

// GormNode 对应数据库中的 'node' 表 (节点表) - 保留用于可能的元数据查询
type GormNode struct {
	ID int32     `gorm:"column:ID;primaryKey"`    // 点标识
	UD int64     `gorm:"column:UD"`               // UUID
	ND int32     `gorm:"column:ND"`               // 父节点标识
	PN string    `gorm:"column:PN;type:char(24)"` // 名称
	ED string    `gorm:"column:ED;type:char(60)"` // 描述
	FQ int32     `gorm:"column:FQ"`               // 分辨率
	LC int32     `gorm:"column:LC"`               // 报警类型
	AR int8      `gorm:"column:AR"`               // 存档
	OF int8      `gorm:"column:OF"`               // 离线
	CT time.Time `gorm:"column:CT"`               // 修改时间
	GN string    `gorm:"column:GN"`               // 全局名称
}

// TableName 指定 GormNode 结构体对应的数据库表名
func (GormNode) TableName() string {
	return "node"
}

// GormRealtime 对应数据库中的 'realtime' 表 (实时表) - 保留用于可能的元数据查询
type GormRealtime struct {
	ID int32     `gorm:"column:ID;primaryKey"` // 测点 ID (假设为主键)
	GN string    `gorm:"column:GN"`            // 测点名称
	TM time.Time `gorm:"column:TM"`            // 测点更新时间
	DS int16     `gorm:"column:DS"`            // 测点状态
	AV []byte    `gorm:"column:AV;type:blob"`  // 测点数值
}

// TableName 指定 GormRealtime 结构体对应的数据库表名
func (GormRealtime) TableName() string {
	return "realtime"
}

// GormArchive 对应数据库中的 'archive' 表 (历史表) - 保留用于可能的元数据查询
type GormArchive struct {
	ID int32     `gorm:"column:ID"`           // 测点 ID (可能与 TM 组成复合主键/索引)
	GN string    `gorm:"column:GN"`           // 测点名称
	TM time.Time `gorm:"column:TM"`           // 测点数据更新时间 (可能与 ID 组成复合主键/索引)
	DS int16     `gorm:"column:DS"`           // 测点状态
	AV []byte    `gorm:"column:AV;type:blob"` // 测点数值
}

// TableName 指定 GormArchive 结构体对应的数据库表名
func (GormArchive) TableName() string {
	return "archive"
}

// GormStat 对应数据库中的 'stat' 表 (历史统计表) - 保留用于可能的元数据查询
type GormStat struct {
	ID      int32     `gorm:"column:ID"`      // 测点 ID (可能与 TM, INTERVAL 组成复合主键/索引)
	GN      string    `gorm:"column:GN"`      // 测点名称
	TM      time.Time `gorm:"column:TM"`      // 测点更新时间 (可能与 ID, INTERVAL 组成复合主键/索引)
	DS      int16     `gorm:"column:DS"`      // 测点状态
	FLOW    float64   `gorm:"column:FLOW"`    // 累积值
	AVGV    float64   `gorm:"column:AVGV"`    // 时均平均值
	MAXV    float64   `gorm:"column:MAXV"`    // 最大值
	MINV    float64   `gorm:"column:MINV"`    // 最小值
	MAXTIME time.Time `gorm:"column:MAXTIME"` // 最大值时间
	MINTIME time.Time `gorm:"column:MINTIME"` // 最小值时间
}

// TableName 指定 GormStat 结构体对应的数据库表名
func (GormStat) TableName() string {
	return "stat"
}

// GormAlarm 对应数据库中的 'alarm' 表 (实时报警表) - 保留用于可能的元数据查询
type GormAlarm struct {
	ID int32     `gorm:"column:ID"`           // 测点 ID (主键/索引待定)
	GN string    `gorm:"column:GN"`           // 测点名称
	RT int8      `gorm:"column:RT"`           // 测点类型
	AL int8      `gorm:"column:AL"`           // 报警优先级
	AC int32     `gorm:"column:AC"`           // 报警颜色
	TF time.Time `gorm:"column:TF"`           // 首次报警时间
	TA time.Time `gorm:"column:TA"`           // 报警时间 (主键/索引待定)
	TM time.Time `gorm:"column:TM"`           // 测点更新时间 (主键/索引待定)
	DS int16     `gorm:"column:DS"`           // 测点状态
	AV []byte    `gorm:"column:AV;type:blob"` // 测点数值
}

// TableName 指定 GormAlarm 结构体对应的数据库表名
func (GormAlarm) TableName() string {
	return "alarm"
}

// GormAAlarm 对应数据库中的 'aalarm' 表 (历史报警表) - 保留用于可能的元数据查询
type GormAAlarm struct {
	ID int32     `gorm:"column:ID"`           // 测点 ID (主键/索引待定)
	GN string    `gorm:"column:GN"`           // 测点名称
	RT int8      `gorm:"column:RT"`           // 测点类型
	AL int8      `gorm:"column:AL"`           // 报警优先级
	AC int32     `gorm:"column:AC"`           // 报警颜色
	TF time.Time `gorm:"column:TF"`           // 首次报警时间
	TA time.Time `gorm:"column:TA"`           // 报警时间 (主键/索引待定)
	TM time.Time `gorm:"column:TM"`           // 测点更新时间 (主键/索引待定)
	DS int16     `gorm:"column:DS"`           // 测点状态
	AV []byte    `gorm:"column:AV;type:blob"` // 测点数值
}

// TableName 指定 GormAAlarm 结构体对应的数据库表名
func (GormAAlarm) TableName() string {
	return "aalarm"
}

// GormUser 对应数据库中的 'user' 表 (用户表) - 保留用于可能的元数据查询
type GormUser struct {
	US string `gorm:"column:US;type:text;primaryKey"` // 用户信息 (假设为主键)
	PW string `gorm:"column:PW;type:text"`            // 用户密码
}

// TableName 指定 GormUser 结构体对应的数据库表名
func (GormUser) TableName() string {
	return "user"
}

// GormGroup 对应数据库中的 'groups' 表 (资源组表) - 保留用于可能的元数据查询
type GormGroup struct {
	ID int    `gorm:"column:ID;primaryKey"` // 资源组 ID
	GP string `gorm:"column:GP;type:text"`  // 资源组信息
}

// TableName 指定 GormGroup 结构体对应的数据库表名
func (GormGroup) TableName() string {
	return "groups"
}

// GormAccess 对应数据库中的 'access' 表 (权限表) - 保留用于可能的元数据查询
type GormAccess struct {
	US string `gorm:"column:US;type:text;primaryKey"` // 用户信息 (复合主键)
	GP int    `gorm:"column:GP;primaryKey"`           // 资源组 (复合主键)
	PL string `gorm:"column:PL;type:text"`            // 权限信息
}

// TableName 指定 GormAccess 结构体对应的数据库表名
func (GormAccess) TableName() string {
	return "access"
}
