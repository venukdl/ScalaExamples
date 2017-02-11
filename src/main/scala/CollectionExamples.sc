//When we work with a network, flattening could benefit some graphs:
val myFriends = List("Monica", "Ross")
val monicaFriends = List("Rachel", "Phoebe")
val rossFriends = List("Rachel", "Chandler", "Joy")

val friendsOfFriends = List( monicaFriends, rossFriends)

val uniqueFriends = friendsOfFriends.flatten.distinct
uniqueFriends
//TIll Here

