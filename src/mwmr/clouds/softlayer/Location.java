package mwmr.clouds.softlayer;

public enum Location {

	PARIS, SYDNEY, MELBOURNE, SINGAPORE, DALLAS, TORONTO, LONDON, SAN_JOSE, MILAN, FRANKFURT, TOKYO, HONG_KONG, AMSTERDAM, MONTREAL, MEXICO;

	public String getTag(){
		switch (this) {
		case PARIS:
			return "par01";
		case SYDNEY:
			return "syd01";
		case MELBOURNE:
			return "mel01";
		case MONTREAL:
			return "mon01";
		case MEXICO:
			return "mex01";
		case SINGAPORE:
			return "sng01";
		case DALLAS:
			return "dal05";
		case TORONTO:
			return "tor01";
		case LONDON:
			return "lon02";
		case SAN_JOSE:
			return "sjc01";
		case MILAN:
			return "mil01";
		case FRANKFURT:
			return "fra02";
		case TOKYO:
			return "tok02";
		case HONG_KONG:
			return "hkg02";
		case AMSTERDAM:
			return "ams01";
		default:
			break;
		}
		return null;
	}

}
