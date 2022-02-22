
* [ ] Update the connection protocol to send the known replicas upon connection
	* This is to eliminate the need for knowing exactly what IPs are alive. You just need to know a couple backups and the rest will be shared during runtime (to prevent stale IP addrs)
* [ ] Sync across replicas
	* Open another RPC port to allow for peer to peer connection. 
	* Send CHANGES over the RPC
		* Possibility to just send a changelog or just to do it live(potential data redundency issues here)
* [ ] Add subscribing to a key to allow for more complex apps & potential PUB/SUB capabilities
