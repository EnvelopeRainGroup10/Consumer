package sqlmodule

import (
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"log"
)

type User struct {
	ID    int64
	Count int64
}

func (User) TableName() string {
	return "user"
}

type Envelope struct {
	ID         int64 `json:"envelope_id"`
	UID        int64 `json:"uid"`
	Opened     bool  `json:"opened"`
	Value      int64 `json:"value"`
	SnatchTime int64 `json:"snatch_time"`
}

func (Envelope) TableName() string {
	return "red_envelope"
}

var DB *gorm.DB

const DbType string = "mysql"
const DbAddress string = "root:123456@(180.184.71.7:8066)/envelope_db?charset=utf8&parseTime=True&loc=Local"

func InitDB() (*gorm.DB, error) {
	db, err := gorm.Open(DbType, DbAddress)
	if err == nil {
		DB = db
		//db.AutoMigrate(&User{}, &Envelope{})
		return db, err
	}
	log.Println(err)
	return nil, err
}

// GetUser 查询用户并返回，如果不存在则创建用户
func GetUser(uid int64) (user User) {
	DB.FirstOrCreate(&user, User{ID: uid})
	return
}

// UpdateCountByUid 根据用户id更新count
func UpdateCountByUid(uid int64) {
	DB.Model(&User{}).Where(User{ID: uid}).Update("count", gorm.Expr("count + ?", 1))
}


// CreateEnvelopeDetail 根据redis中的红包id, 用户id, 红包价值value 与创建时间snatch_time创建红包记录
func CreateEnvelopeDetail(envelopeId int64, uid int64, value int64, snatchTime int64) (envelope Envelope) {

	envelope = Envelope{ID: envelopeId, UID: uid, Opened: false, Value: value, SnatchTime: snatchTime}
	if err := DB.Create(&envelope).Error; err != nil {
		return Envelope{ID: 0}
	}
	return envelope
}

// UpdateStateByEidAndUid 根据用户id与红包id更新红包状态，将红包设置为已经打开状态
func UpdateStateByEidAndUid(envelopeId int64, uid int64) {
	envelope:=Envelope{ID: envelopeId,UID: uid}
	DB.Model(&envelope).Where(envelope).Update("opened", true)
}