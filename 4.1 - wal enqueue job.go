package main

func (pool *dacV3WorkerWriter) WriteDirect(idDataArena int64, data []byte, offset int64) error {

	j := &jobWriter{
		direct:      true,
		offset:      offset,
		idDataArena: idDataArena,
		data:        data,
		resp:        make(chan error, 1),
	}

	select {
	case pool.jobs <- j:

		select {
		case err := <-j.resp:
			return err
		case <-pool.ctx.Done():
			return pool.ctx.Err()
		}

	case <-pool.ctx.Done():
		return pool.ctx.Err()
	}
}

func (pool *dacV3WorkerWriter) WriteWall(idDataArena int64, data []byte, offset int64) error {

	j := &jobWriter{
		direct:      false,
		offset:      offset,
		idDataArena: idDataArena,
		data:        data,
		resp:        make(chan error, 1),
	}

	select {
	case pool.jobs <- j:

		select {
		case err := <-j.resp:
			return err
		case <-pool.ctx.Done():
			return pool.ctx.Err()
		}

	case <-pool.ctx.Done():
		return pool.ctx.Err()
	}
}

func (pool *dacV3WorkerWriter) WriteUnSafe(j *jobWriter) {

	j.directIo = true

	j.direct = false

	j.bufIdx = 0

	//Esto se borra al escribir primera vez en el wal
	j.idIndexArena = 0
	//EStas variables solo se usan al escribir en el buffer
	j.indexOffsetStart = 0
	j.indexOffsetEnd = 0

	//estas varialbles  se usan en writedisk , hay que ponerlas a cero para no modificar lo que escribe de buffer
	j.dataOffsetStart = 0
	j.dataOffsetEnd = 0

	//Deberia estar compleatdo
	//wg     sync.WaitGroup

	//ESto deberia estar completado ya
	//resp chan error

	//debug
	if j.resp != nil {
		println("ERROR FATAL - WriteUnSafe - RESPUESTA NO ES NIL")
	}

	//No se modifican hace falta para escribir directo
	//offset
	//idDataArena int64
	//data        []byte

	pool.jobs <- j

	return
}
