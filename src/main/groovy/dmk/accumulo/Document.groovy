package dmk.accumulo

public class Document{

    protected String dtg
    protected String title
    protected String summary
    protected String name
	protected String uri
    protected String contents
    
    @Override
    public String toString(){
        return "$uri $dtg $name $title $summary"
    }
}