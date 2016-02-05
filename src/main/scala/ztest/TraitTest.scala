/**
 * User: hanchensu
 * Date: 2014-02-21
 * Time: 10:13
 */
package ztest

trait ppLogged {
	println("logged")
	def log(msg: String) { print("0:"+msg)}
}

trait pLogged1 extends ppLogged {
//	override def log(msg: String) {
//		println("1:"+ msg)
//	}
}

trait pLogged2 extends ppLogged {
	override def log(msg: String) {
		println("2:"+ msg)
	}
}

trait Logged1 extends ppLogged {
	override def log(msg: String) {
		super.log("prefix_" + msg)
	}
}

class Test extends pLogged1 with Logged1 {
	def test() {println("test")}
	def print(msg:String) {log(msg)}
}

object TraitTest {
	def main(args: Array[String]) {
		val acct1 = new Test
		acct1.print("teststring")

	}
}
