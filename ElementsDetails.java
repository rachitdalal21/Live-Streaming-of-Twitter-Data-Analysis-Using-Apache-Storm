public class ElementsDetails {

    public long frequency;
    public long error;

    ElementsDetails( long delta ) {
        this.frequency = 1;
        this.error = delta;
    }
    public void increment() {
        frequency++;
    }


}
