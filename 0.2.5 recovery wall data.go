package dacV3

func startRecoveryWallData(sfDacV3 *DacV3, pendingUpdates []WalUpdateEntry) {

	for _, entry := range pendingUpdates {

		err := sfDacV3.WriteIfExistPage(entry.Hash, entry.Data, entry.OffsetRelative)
		if err != nil {
			println("ERROR: startRecoveryWallData - ", err.Error())
		}

	}

}
