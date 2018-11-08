package top.wdzaslzy.bean

/**
  * @author lizy
  **/
case class AdClickLog() extends Serializable {
  private var sessionId = ""
  private var advertisersId = -1
  private var advertisementId = -1
  private var putInType = -1
  private var adPrice = 0D
  private var requestTime = ""
  private var requestIp = ""
  private var city = ""
  private var province = ""
  private var isEffective = -1

  //统计
  private var total = 0 //总竞价数
  private var totalEffective = 0 //有效数
  private var effectiveRate = 0.0 //成功率
  private var showTypeTotal = 0 //展示量
  private var clickTypeTotal = 0 //点击量
  private var clickRate = 0.0     //点击率
  private var totalPrice = 0.0 //广告成本（有效）

  def setSessionId(session: String): AdClickLog = {
    this.sessionId = sessionId
    this
  }

  def getSessionId: String = this.sessionId

  def setAdvertisersId(advertisersId: Int): AdClickLog = {
    this.advertisersId = advertisersId
    this
  }

  def getAdvertisersId: Int = this.advertisersId

  def setAdvertisementId(advertisementId: Int): AdClickLog = {
    this.advertisementId = advertisementId
    this
  }

  def getAdvertisementId: Int = this.advertisementId

  def setPutInType(putInType: Int): AdClickLog = {
    this.putInType = putInType
    this
  }

  def getPutInType: Int = this.putInType

  def setAdPrice(adPrice: Double): AdClickLog = {
    this.adPrice = adPrice
    this
  }

  def getAdPrice: Double = this.adPrice

  def setRequestTime(requestTime: String): AdClickLog = {
    this.requestTime = requestTime
    this
  }

  def getRequestTime: String = this.requestTime

  def setRequestIp(requestIp: String): AdClickLog = {
    this.requestIp = requestIp
    this
  }

  def getRequestIp: String = this.requestIp

  def setCity(city: String): AdClickLog = {
    this.city = city
    this
  }

  def getCity: String = this.city

  def setProvince(province: String): AdClickLog = {
    this.province = province
    this
  }

  def getProvince: String = this.province

  def setEffective(isEffective: Int): AdClickLog = {
    this.isEffective = isEffective
    this
  }

  def getIsEffective: Int = this.isEffective


  ////////////
  def getTotal: Int = this.total

  def addTotal(value: Int): Unit = this.total + value

  def getTotalEffective: Int = this.totalEffective

  def addTotalEffective(value: Int): Unit = this.totalEffective + value

  def getEffectiveRate: Double = this.effectiveRate

  def setEffectiveRate(value: Double): Unit = this.effectiveRate = value

  def getShowTypeTotal: Int = this.showTypeTotal

  def addShowTypeTotal(value: Int): Unit = this.showTypeTotal + value

  def getClickTypeTotal: Int = this.clickTypeTotal

  def addClickTypeTotal(value: Int): Unit = this.clickTypeTotal + value

  def getClickRate: Double = this.clickRate

  def setClickRate(value: Double): Unit = this.clickRate = value

  def getTotalPrice: Double = this.totalPrice

  def setTotalPrice(value: Double): Unit = this.totalPrice = value

  override def toString: String =
    s"""
       |total:$total,
       |totalEffective:$totalEffective
       |effectiveRate:$effectiveRate
       |showTypeTotal:$showTypeTotal
       |clickTypeTotal:$clickTypeTotal
       |clickRate:$clickRate
       |totalPrice:$totalPrice
     """.stripMargin
}
