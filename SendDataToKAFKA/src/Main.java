
public class Main {

	public static void main(String args[]) {
		Producer producer=new Producer();
		producer.run();
		try {
			Thread.sleep(200);
			Emmitter emmitter=new Emmitter(producer);
			emmitter.run();
			producer.join();
			emmitter.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
}
