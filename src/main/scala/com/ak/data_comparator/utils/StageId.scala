package com.ak.data_comparator.utils

object StageId {
  var stageId = 0

  def getNextStageId = {
    stageId = stageId + 1
    stageId
  }
}