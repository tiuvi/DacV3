package dacV3

func startRecoveryWallData(sfDacV3 *DacV3, pendingUpdates []WalUpdateEntry) {

	for _, entry := range pendingUpdates {

		//println("startRecoveryWallData: " , gettLastLine(string(entry.Data)) , " offset: ",entry.OffsetRelative)
		err := sfDacV3.WriteIfExistPage(entry.Hash, entry.Data, entry.OffsetRelative)
		if err != nil {
			println("ERROR: startRecoveryWallData - ", err.Error())
		}

	}

}
