package com.ica.gruppen.web2hdfs

object CaseClassDefns {

  case class Parse1Data6(year: Int, month: Int, day: Int, mrng: Float, aftn: Float, evng: Float)
  case class Parse1Data8(year: Int, month: Int, day: Int, mrng: Float, aftn: Float, evng: Float, tmax: Float, tmin: Float)
  case class Parse1Data9(year: Int, month: Int, day: Int, mrng: Float, aftn: Float, evng: Float, tmax: Float, tmin: Float, tmean: Float)

  case class Parse2Data9(year: Int, month: Int, day: Int, b_mrng: Float, bt_mrng: Float, b_aftn: Float, bt_aftn: Float, b_evng: Float, bt_evng: Float)
  case class Parse2Data12(year: Int, month: Int, day: Int, b_mrng: Float, t_mrng: Float, a_mrng: Float, b_aftn: Float, t_aftn: Float, a_aftn: Float, b_evng: Float, t_evng: Float, a_evng: Float)
  case class Parse2Data6(year: Int, month: Int, day: Int, a_mrng: Float, a_aftn: Float, a_evng: Float)

}