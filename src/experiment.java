import java.util.ArrayList;


public class experiment {

	public static void main(String[] args) {
		ArrayList<Integer> exp=new ArrayList<Integer>();
		ArrayList<Integer> aux;
		aux=exp;
		for(int i=0;i<10;i++){
			exp.add(i);
		}
		
		exp.remove(0);
		exp.remove(0);
		System.out.println(aux.size()+" value at 0: "+exp.get(0));
	}

}

